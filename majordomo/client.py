import zmq
import functools
from errno import ETIME, EPROTO
from mdp import constants as consts
from mdp import MDPError
from stream import mdp_stream
from utils import verbose


class Client(object):
    socket = None

    def __init__(self, zmq_ctx, broker):
        self.zmq_ctx = zmq_ctx
        self.broker = broker
        self.poller = zmq.Poller()
        self.reconnect()

    def reconnect(self):
        self.close()
        self.socket = self.zmq_ctx.socket(zmq.REQ)
        self.socket.connect(self.broker)
        self.poller.register(self.socket, zmq.POLLIN)

    def request(self, service, msg, timeout=3.5, retry=0):
        assert retry >= 0

        req = [ consts.client, service ]
        if isinstance(msg, list):
            req.extend(msg)
        else:
            req.append(msg)

        for i in xrange(0, retry + 1):
            self.socket.send_multipart(req)
            verbose(req)
            items = self.poller.poll(timeout * 1000)
            if items:
                msg = self.socket.recv_multipart()
                verbose(msg)
                if len(msg) < 3:
                    raise MDPError('Partial reply')
                verbose(msg)
                protocol, rep_service = msg[0:2]
                if protocol != consts.client or rep_service != service:
                    raise MDPError('Mismatched reply')
                del msg[:2]
                return msg
            else:
                self.reconnect()
        raise OSError(ETIME, 'Request timeout')


    def discovery(self, service):
        return self.request('mmi.service', service)

    def close(self):
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
        pass


class AsyncClient(object):
    stream = None

    def __init__(self, zmq_ctx, broker):
        self.zmq_ctx = zmq_ctx
        self.broker = broker

        socket = self.zmq_ctx.socket(zmq.DEALER)
        self.stream = mdp_stream(socket)
        self.stream.connect(self.broker)

    def request(self, service, msg, callback, timeout=3.5, retry=0):
        """
        Request a service asynchronously.

        Request for a `service` within `msg`. `callback` will be invoked asynchronously
        when reponse is received, reponse is malformed, or timeout occured.

        Signature of `callback` is `callback(error_code, msg)`.

        If error_code is None, then response is passed as `msg`. Otherwise, error_code
        is an instance of OSError.
        """
        assert retry >= 0

        req = [ b'', consts.client, service ]
        if isinstance(msg, list):
            req.extend(msg)
        else:
            req.append(msg)

        # Bind parameters
        on_timeout = functools.partial(self.on_timeout, callback)
        deadline = self.stream.io_loop.time() + timeout

        timeout_hdl = self.stream.io_loop.add_timeout(deadline, on_timeout)

        on_recv = functools.partial(self.on_recv, callback, service, timeout_hdl)
        self.stream.on_recv(on_recv)

        self.stream.send_multipart(req)
        verbose(req)
        pass

    def on_recv(self, callback, service, timeout_hdl, msg):
        verbose(msg)
        self.stream.io_loop.remove_timeout(timeout_hdl)
        ec = None
        try:
            if len(msg) < 4:
                raise MDPError('Partial reply')
            _, protocol, rep_service = msg[0:3]
            if protocol != consts.client or rep_service != service:
                raise MDPError('Mismatched reply')
            del msg[:3]
        except MDPError as e:
            ec = OSError(EPROTO, e.message)
        callback(ec, msg)
        pass

    def on_timeout(self, callback):
        self.stream.stop_on_recv()
        callback(OSError(ETIME, 'Request timeout'), None)
        pass

    def close(self):
        self.stream.close()
        pass


