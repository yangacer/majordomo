import zmq
import functools
from mdp import constants as consts
from zmq.eventloop.zmqstream import ZMQStream
from weakref import ref as wref
from safe_ref import safe_ref
from utils import verbose

class mdp_stream(ZMQStream):

    client_msg          = [ b'', consts.client ]
    ready_msg           = [ b'', consts.worker, consts.command.ready ]
    request_msg         = [ b'', consts.worker, consts.command.request ]
    reply_msg           = [ b'', consts.worker, consts.command.reply ]
    heartbeat_msg       = [ b'', consts.worker, consts.command.heartbeat ]
    disconnect_msg      = [ b'', consts.worker, consts.command.disconnect ]

    def __init__(self, socket):
        super(mdp_stream, self).__init__(socket)
        socket.setsockopt(zmq.LINGER, 0)
        pass

    def combine_(self, *arg):
        head = []
        for ele in arg:
            assert isinstance(ele, list) or isinstance(ele, str)
            if isinstance(ele, str):
                head.append(ele)
            else:
                head.extend(ele)
        return head

    def snd_to_client(self, addr, service, msg):
        fulltext = self.combine_(addr, self.client_msg, service, msg)
        self.send_multipart(fulltext)
        verbose(fulltext)

    def snd_ready(self, service):
        fulltext = self.ready_msg + [service]
        self.send_multipart(fulltext)
        verbose(fulltext)

    def snd_heartbeat(self, address=None):
        if address is None:
            self.send_multipart(self.heartbeat_msg)
            verbose(self.heartbeat_msg)
        else:
            fulltext = [address] + self.heartbeat_msg
            self.send_multipart(fulltext)
            verbose(fulltext)

    def snd_request(self, address, service, msg):
        fulltext = self.combine_(address,  self.request_msg, service, msg)
        self.send_multipart(fulltext)
        verbose(fulltext)

    def snd_reply(self, addr, msg):
        fulltext = self.combine_(self.reply_msg, [addr, b''], msg)
        self.send_multipart(fulltext)
        verbose(fulltext)

    def snd_disconnect(self, addr):
        fulltext = None
        if addr is None:
            fulltext = self.disconnect_msg
        else:
            fulltext = self.combine_(addr, self.disconnect_msg)
        self.send_multipart(fulltext)
        verbose(fulltext)


class heartbeat_stream(mdp_stream):

    def on_heartbeat(self, callback, interval, internal_=None):
        if not self.closed():
            if internal_:
                callback()
            on_timeout = functools.partial(self.on_heartbeat, callback, interval, 1)
            self.call_later(interval, on_timeout)
        pass

    # Define call_later only if it's not available
    #if not hasattr(mdp_stream.io_loop, 'call_later'):
    def call_later(self, delta, callback):
        at = self.io_loop.time() + delta
        return self.io_loop.add_timeout(at, callback)


