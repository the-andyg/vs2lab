import random
import logging

# coordinator messages
from const2PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT, PREPARE_COMMIT
# participant decissions
from const2PC import LOCAL_SUCCESS, LOCAL_ABORT
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT, NEED_DECISION, READY_COMMIT
# misc constants
from const2PC import TIMEOUT

import stablelog


class Participant:
    """
    Implements a two phase commit participant.
    - state written to stable log (but recovery is not considered)
    - in case of coordinator crash, participants mutually synchronize states
    - system blocks if all participants vote commit and coordinator crashes
    - allows for partially synchronous behavior with fail-noisy crashes
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log(
            "participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.2pc.Participant")
        self.coordinator = {}
        self.all_participants = {}
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not
        #ursprünglich 2/3 wahrscheinlichkeit
        return LOCAL_ABORT if random.random() > 2/3 else LOCAL_SUCCESS

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Participant {} entered state {}."
                         .format(self.participant, state))
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self._enter_state('INIT')  # Start in local INIT state.

    def run(self):
        # Wait for start of joint commit
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:  # Crashed coordinator - give up entirely
            self.determineCoordinator()
            return
            # decide to locally abort (before doing anything)
            #decision = LOCAL_ABORT
            #decision = self.state

        else:  # Coordinator requested to vote, joint commit starts
            assert msg[1] == VOTE_REQUEST

            # Firstly, come to a local decision
            decision = self._do_work()  # proceed with local activities

            # If local decision is negative,
            # then vote for abort and quit directly
            if decision == LOCAL_ABORT:
                self.channel.send_to(self.coordinator, VOTE_ABORT)

            # If local decision is positive,
            # we are ready to proceed the joint commit
            else:
                assert decision == LOCAL_SUCCESS
                self._enter_state('READY')

                # Notify coordinator about local commit vote
                self.channel.send_to(self.coordinator, VOTE_COMMIT)

                # Wait for coordinator to notify the final outcome
                msg=False
                while not msg:
                    msg = self.channel.receive_from(self.coordinator, TIMEOUT)
                    if not msg:  # Crashed coordinator
                        self.determineCoordinator()
                    else:
                        decision = msg[1]

        # Change local state based on the outcome of the joint commit protocol
        # Note: If the protocol has blocked due to coordinator crash,
        # we will never reach this point
        if decision == PREPARE_COMMIT:
            self._enter_state('PRECOMMIT')
        else:
            assert decision in [GLOBAL_ABORT, LOCAL_ABORT]
            self._enter_state('ABORT')
            return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
                
        self.channel.send_to(self.coordinator, READY_COMMIT)

        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:  # Crashed coordinator - give up entirely
            self.determineCoordinator()
            # decide to locally abort (before doing anything)
            #decision = LOCAL_ABORT

        else:  # Coordinator requested to vote, joint commit starts
            print(str(msg[1]))
            assert msg[1] == GLOBAL_COMMIT
            decision = GLOBAL_COMMIT
            self._enter_state('COMMIT')
            
        return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
    
    def determineCoordinator(self):
        min = 1000000
        id = self.participant
        for participant in self.all_participants:
            if int(participant) < int(min):
                min = participant
        self.coordinator = min
        self.all_participants.remove(self.coordinator)
        print("new coordinator: "+self.coordinator)
        if self.coordinator == id:
            if(self.state=="INIT" or self.state=="READY"):
                self.beginInit()
            elif self.state=="ABORT":
                print("Koordinator in abort")
                self.globalAbortState()
            else:
                self.globalCommitState()
        
    def beginInit(self):
        # Request local votes from all participants
        self._enter_state('WAIT')
        self.channel.send_to(self.all_participants, VOTE_REQUEST)
        return self.readyState()
        
    def readyState(self):
        # Collect votes from all participants
        selfResult= self._do_work()
        if selfResult == False:
            reason = "local_abort from new coordinator"
            return self.globalAbortState(reason)
        yet_to_receive = list(self.all_participants)
        while len(yet_to_receive) > 0:
            msg = self.channel.receive_from(self.all_participants, TIMEOUT)

            if (not msg) or (msg[1] == VOTE_ABORT):
                reason = "timeout" if not msg else "local_abort from " + msg[0]
                return self.globalAbortState(reason)

            else:
                assert msg[1] == VOTE_COMMIT
                yet_to_receive.remove(msg[0])

        self._enter_state('PRECOMMIT')
        self.channel.send_to(self.all_participants, PREPARE_COMMIT)
        yet_to_receive = list(self.all_participants)
        while len(yet_to_receive) > 0:
            msg = self.channel.receive_from(self.all_participants, TIMEOUT)
            if (not msg):
                reason = "timeout"
                return self.globalCommitState()
            else:
                assert msg[1] == READY_COMMIT
                yet_to_receive.remove(msg[0])
        return self.globalCommitState()
    
    def globalCommitState(self):
        self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
        return "Coordinator {} terminated in state COMMIT."\
            .format(self.coordinator)
            
    def globalAbortState(self,reason):
        print("neue koordinator global abort")
        self._enter_state('ABORT')
        self.channel.send_to(self.all_participants, GLOBAL_ABORT)
        return "Coordinator {} terminated in state ABORT. Reason: {}."\
                    .format(self.coordinator, reason)
