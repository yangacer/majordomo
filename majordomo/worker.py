import zmq
import logging
from stream import heartbeat_stream
from mdp import constants as consts
from mdp import MDPError
from utils import verbose


class Worker(object):

    stream = None
    service_name = None
    broker = None
    init_liveness_ = None
    curr_liveness_ = 0

    def __init__(self, zmq_ctx, broker, service_name,
                 hb_interval=2.0, hb_liveness=3):

        self.zmq_ctx = zmq_ctx
        self.service_name = service_name
        self.broker = broker
        self.init_liveness_ = hb_liveness

        socket = self.zmq_ctx.socket(zmq.DEALER)
        self.stream = heartbeat_stream(socket)
        self.stream.on_heartbeat(self.heartbeat, hb_interval)
        self.stream.on_recv(self.on_recv)
        self.recover()

    def heartbeat(self):
        self.curr_liveness_ -= 1
        if self.curr_liveness_ < 0:
            self.recover()
        self.stream.snd_heartbeat()
        pass

    def recover(self):
        self.stream.io_loop.add_callback(self.stream.connect, self.broker)
        self.stream.snd_ready(self.service_name)
        self.reset_liveness()

    def reset_liveness(self):
        self.curr_liveness_ = self.init_liveness_

    def on_recv(self, msg):
        verbose(msg)
        try:
            if len(msg) < 3:
                raise MDPError('Partial worker message')
            _, protocol, cmd = msg[:3]
            if _ != '' or protocol != consts.worker:
                raise MDPError('Invalid worker protocol format')
            if cmd == consts.command.request:
                if len(msg) < 6:
                    raise MDPError('Short request')
                addr, _ = msg[4:6]
                if _ != '':
                    raise MDPError('Unexpected frame delimiter')
                del msg[:6]
                # TODO Incremental reply
                self.stream.snd_reply(addr, self.on_request(msg))
                del msg
            elif cmd == consts.command.disconnect:
                self.recover()
                return
            elif cmd == consts.command.heartbeat:
                pass
            else:
                raise MDPError('Unknown command')
        except MDPError as e:
            logging.getLogger('mdp').exception(e)
            pass
        self.reset_liveness()

        pass

    def on_request(self, msg):
        raise NotImplementedError('Method on_request must be overrided')

    def close(self):
        self.stream.close()
