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
                msg = self.channel.receive_from(self.coordinator, TIMEOUT)

                if not msg:  # Crashed coordinator
                    self.determineCoordinator()
                    return

                else:  # Coordinator came to a decision
                    decision = msg[1]

        # Change local state based on the outcome of the joint commit protocol
        # Note: If the protocol has blocked due to coordinator crash,
        # we will never reach this point
        if decision == PREPARE_COMMIT:
            self.channel.send_to(self.coordinator, READY_COMMIT)
            self._enter_state('PRECOMMIT')
        else:
            assert decision in [GLOBAL_ABORT, LOCAL_ABORT]
            self._enter_state('ABORT')
            return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
        
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)
        if not msg:  # Crashed coordinator
            self.determineCoordinator()
            return
        else:  # Coordinator came to a decision
            decision = msg[1]
            assert decision in [GLOBAL_COMMIT]
            self._enter_state('COMMIT')
            return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
        

    def determineCoordinator(self):
            min = 1000000
            id = self.participant
            for participant in self.all_participants:
                if int(participant) < int(min):
                    min = participant
            print("coordinator vorher: "+str(self.coordinator))
            self.coordinator={min}
            print("coordinator nachher: "+str(self.coordinator))
            self.all_participants.remove(min)
            print("new coordinator: "+min)
            if self.coordinator == {id}:
                self.channel.send_to(self.all_participants, self.state)
                if(self.state != "PRECOMMIT"):
                    yet_to_receive = list(self.all_participants)
                    print("warten auf alle")
                    while len(yet_to_receive) > 0:
                        msg = self.channel.receive_from(self.all_participants, TIMEOUT)
                        if (not msg):
                            print("timeout")
                            break
                        else:
                            print("antwort von participant")
                            assert msg[1] == VOTE_ABORT or msg[1]==VOTE_COMMIT
                            yet_to_receive.remove(msg[0])
                    self._enter_state('ABORT')
                    self.channel.send_to(self.all_participants, GLOBAL_ABORT)
                    print( "Participant {} terminated in state {} due to {}.".format(
                        self.participant, self.state, "GLOBAL_ABORT"))
                else:
                    yet_to_receive = list(self.all_participants)
                    while len(yet_to_receive) > 0:
                        msg = self.channel.receive_from(self.all_participants, TIMEOUT)
                        if (not msg):
                            print("timeout")
                            break
                        else:
                            assert msg[1] == READY_COMMIT
                            yet_to_receive.remove(msg[0])
                    self._enter_state('COMMIT')
                    self.channel.send_to(self.all_participants, GLOBAL_COMMIT)
                    print( "Participant {} terminated in state {} due to {}.".format(
                        self.participant, self.state, "GLOBAL_COMMIT"))
            else:
                msg = self.channel.receive_from(self.coordinator,5)
                if(not msg):
                    print("Koordinator vorher local abort")
                    self.determineCoordinator()
                    return
                if msg[1]=="INIT":
                    self.channel.send_to(self.coordinator, VOTE_COMMIT)
                    print("init send")
                elif msg[1]=="READY":
                    self.channel.send_to(self.coordinator, VOTE_COMMIT)
                    print("wait send")
                elif msg[1]=="ABORT":
                    self.channel.send_to(self.coordinator, VOTE_ABORT)
                    print("abort send")
                else:
                    print("STATE of Coordinator not INIT, WAIT or ABORT, new State: " + msg[1])

                msg = self.channel.receive_from(self.coordinator, 5)
                if(msg[1]==GLOBAL_COMMIT):
                    self._enter_state('COMMIT')
                    print( "Participant {} terminated in state {} due to {}.".format(
                        self.participant, self.state, "GLOBAL_COMMIT"))
                else:
                    self._enter_state('ABORT')
                    print( "Participant {} terminated in state {} due to {}.".format(
                        self.participant, self.state, "GLOBAL_ABORT"))