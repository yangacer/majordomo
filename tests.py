import unittest
import gc, sys
import zmq
import logging
import errno
from cStringIO import StringIO
from binascii import hexlify
from threading import Thread, Timer
from zmq.eventloop.ioloop import IOLoop, DelayedCallback

from majordomo.mdp import constants as consts, MDPError
from majordomo.stream import mdp_stream
from majordomo.worker import Worker
from majordomo.broker import Broker
from majordomo.client import Client, AsyncClient
from majordomo.utils import verbose

from nose_parameterized import parameterized

def setUpModule():
    logger = logging.getLogger('mdp')
    logger.setLevel(logging.DEBUG)
    pass

class EchoWorker(Worker):

    def __init__(self, ctx, broker):
        super(EchoWorker, self).__init__(ctx, broker, 'echo')

    def on_request(self, msg):
        return [b'ECHO'] + msg

class SlowWorker(Worker):

    def __init__(self, ctx, broker):
        super(SlowWorker, self).__init__(ctx, broker, 'slow', 14.00)

class EchoCaller(Client):

    def __init__(self, ctx, broker, msg):
        self.msg = msg
        super(EchoCaller, self).__init__(ctx, broker)

    def __call__(self):
        self.reply = self.request('echo', self.msg)

class DiscoveryService(Client):

    def __init__(self, ctx, broker, service):
        super(DiscoveryService, self).__init__(ctx, broker)
        self.service = service

    def __call__(self):
        self.reply = super(DiscoveryService, self).discovery(self.service)

class AsyncServiceCaller(AsyncClient):
    """
    Send request to a service asynchronously and save reply in self.reply.
    Default value of service parameter is `echo`.
    """
    def __init__(self, ctx, broker, msg, service='echo'):
        super(AsyncServiceCaller, self).__init__(ctx, broker)
        self.request(service, msg, self.on_reply)

    def on_reply(self, error_code, msg):
        self.error_code = error_code
        self.reply = msg

class ServiceCaller(Client):
    reply = None
    exception = None
    def __init__(self, ctx, broker, msg, service='echo'):
        self.service = service
        self.msg = msg
        super(ServiceCaller, self).__init__(ctx, broker)

    def __call__(self):
        try:
            self.reply = self.request('echo', self.msg, retry=1)
        except Exception as e:
            self.exception = e
        pass


class RedundantReadyWorker(Worker):

    msg = None
    def __init__(self, ctx, broker):
        super(RedundantReadyWorker, self).__init__(ctx, broker, 'double_ready')
        self.stream.snd_ready('double_ready')

    def on_recv(self, msg):
        if b'\x05' in msg:
            self.msg = msg
            super(RedundantReadyWorker, self).on_recv(msg)
        else:
            pass
        pass

class DisconnectWorker(Worker):

    def __init__(self, ctx, broker):
        super(DisconnectWorker, self).__init__(ctx, broker, 'disconnect', 4.0)
        self.stream.snd_disconnect(None)

class MalformBrokerForWorker(Broker):

    def __init__(self, zmq_ctx, malform_msg):
        self.malform_msg = malform_msg
        super(MalformBrokerForWorker, self).__init__(zmq_ctx)
        pass

    def process_worker(self, addr, msg):
        command = msg[0]
        if command in [consts.command.reply, consts.command.ready ] :
            super(MalformBrokerForWorker, self).process_worker(addr, msg)

        self.stream.send_multipart([addr] + self.malform_msg)
        pass

class MalformBrokerForClient(Broker):

    def __init__(self, zmq_ctx, malform_msg):
        self.malform_msg = malform_msg
        super(MalformBrokerForClient, self).__init__(zmq_ctx)
        pass

    def process_client(self, addr, msg):
        self.stream.send_multipart([addr, ''] + self.malform_msg)
        pass

class testcases(unittest.TestCase):

    def setUp(self):
        self.zmq_ctx = zmq.Context()
        pass

    def tearDown(self):
        self.zmq_ctx.destroy()
        pass

    def test_print_broker(self):
        broker = Broker(self.zmq_ctx)
        print broker
        broker.close()
        pass

    def test_one_worker(self):
        broker = Broker(self.zmq_ctx)
        worker = EchoWorker(self.zmq_ctx, broker.address[0])

        stopper = DelayedCallback(IOLoop.instance().stop, 10000)
        stopper.start()
        IOLoop.instance().start()
        print broker
        broker.close()
        worker.close()
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.assertTrue('echo' in broker.services)
        self.assertEqual(len(broker.workers), 1)
        self.assertEqual(len(broker.services['echo'].waiting), 1)
        pass

    def test_woker_send_redundant_ready(self):
        broker = Broker(self.zmq_ctx)
        worker = RedundantReadyWorker(self.zmq_ctx, broker.address[0])

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

        ioloop.start()
        broker.close()
        worker.close()
        self.assertEqual(['', 'MDPW01', '\x05'], worker.msg)
        pass

    def test_worker_send_disconnect(self):
        broker = Broker(self.zmq_ctx)

        worker = DisconnectWorker(self.zmq_ctx, broker.address[0])

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 2.0, ioloop.stop)

        ioloop.start()
        self.assertIn('disconnect', broker.services)
        self.assertEqual(len(broker.workers), 0)
        self.assertEqual(len(broker.waiting), 0)
        broker.close()
        worker.close()
        pass

    @parameterized.expand([
        ('incomplete_message', [b'']),
        ('unknown_protocol', [b'', 'unknown']),
        ('invalid_client', [b'', consts.client ]),
        ('invalid_worker', [b'', consts.worker ]),
        ('invalid_worker_commmand', [b'', consts.worker, b'\x0f' ]),
        ('invalid_worker_reply', [b'', consts.worker, consts.command.reply ]),
    ])
    def test_broker_error_resistence(self, name, msg):
        broker = Broker(self.zmq_ctx)
        socket = self.zmq_ctx.socket(zmq.DEALER)
        client = AsyncServiceCaller(self.zmq_ctx, broker.address[0], 'no_such_one', 'mmi.service')
        stream = mdp_stream(socket)
        stream.connect(broker.address[0])

        stream.send_multipart(msg)

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

        ioloop.start()

        self.assertEqual(['404'], client.reply)
        broker.close()
        stream.close()
        client.close()
        pass

    def test_broker_retire_worker_by_invalid_heartbeat(self):

        broker = Broker(self.zmq_ctx)
        socket = self.zmq_ctx.socket(zmq.DEALER)
        stream = mdp_stream(socket)
        stream.connect(broker.address[0])

        class receiver(object):
            reply = None
            def __call__(self, msg):
                self.reply = msg
                pass
            pass

        recver = receiver()

        stream.on_recv(recver)
        stream.send_multipart([b'', consts.worker, consts.command.heartbeat ])

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

        ioloop.start()

        self.assertEqual([b'', consts.worker, consts.command.disconnect], recver.reply)

        broker.close()
        stream.close()
        pass

    def test_slow_worker(self):
        broker = Broker(self.zmq_ctx)
        worker = SlowWorker(self.zmq_ctx, broker.address[0])

        stopper = DelayedCallback(IOLoop.instance().stop, 12000)
        stopper.start()
        IOLoop.instance().start()
        broker.close()
        worker.close()
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.assertTrue('slow' in broker.services)
        self.assertEqual(len(broker.workers), 0)
        self.assertEqual(len(broker.services['slow'].waiting), 0)
        pass

    def test_fast_worker(self):
        broker = Broker(self.zmq_ctx)
        worker = Worker(self.zmq_ctx, broker.address[0], 'fast', 0.2)

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.5, ioloop.stop)

        ioloop.start()

        broker.close()
        worker.close()

        pass

    def test_sync_client_and_EchoWorker(self):
        broker = Broker(self.zmq_ctx)
        worker = EchoWorker(self.zmq_ctx, broker.address[0])
        client = EchoCaller(self.zmq_ctx, broker.address[0], 'HelloEcho')

        proc = Thread(target=client)
        stopper = DelayedCallback(IOLoop.instance().stop, 3000)
        proc.start()

        stopper.start()
        IOLoop.instance().start()
        print broker
        proc.join()
        broker.close()
        worker.close()
        client.close()
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.assertTrue('echo' in broker.services)
        self.assertEqual(len(broker.workers), 1)
        self.assertEqual(len(broker.services['echo'].waiting), 1)
        self.assertEqual(['ECHO', 'HelloEcho'], client.reply)
        pass

    def test_async_client_and_EchoWorker(self):
        broker = Broker(self.zmq_ctx)
        worker = EchoWorker(self.zmq_ctx, broker.address[0])
        client = AsyncServiceCaller(self.zmq_ctx, broker.address[0], 'HelloEcho')

        stopper = DelayedCallback(IOLoop.instance().stop, 1500)
        stopper.start()
        IOLoop.instance().start()
        broker.close()
        worker.close()
        client.close()
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.assertEqual(['ECHO', 'HelloEcho'], client.reply)
        pass

    def test_mmi_service_can_find_existing_service(self):
        broker = Broker(self.zmq_ctx)
        worker = EchoWorker(self.zmq_ctx, broker.address[0])
        client = DiscoveryService(self.zmq_ctx, broker.address[0], 'echo')

        proc = Timer(0.2, client)
        stopper = DelayedCallback(IOLoop.instance().stop, 2000)
        proc.start()

        stopper.start()
        IOLoop.instance().start()
        proc.join()
        broker.close()
        worker.close()
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.assertEqual(['200'], client.reply)
        pass

    def test_mmi_invalid_service(self):
        broker = Broker(self.zmq_ctx)
        client = AsyncServiceCaller(self.zmq_ctx, broker.address[0], b'', b'mmi.invalid')

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 4.0, ioloop.stop)

        ioloop.start()
        self.assertEqual(['501'], client.reply)

        broker.close()
        client.close()

        pass

    def test_mmi_service_with_none_existing_service(self):
        broker = Broker(self.zmq_ctx)
        worker = EchoWorker(self.zmq_ctx, broker.address[0])
        client = DiscoveryService(self.zmq_ctx, broker.address[0], 'no_such_one')

        proc = Thread(target=client)
        stopper = DelayedCallback(IOLoop.instance().stop, 3000)
        proc.start()

        stopper.start()
        IOLoop.instance().start()
        proc.join()
        broker.close()
        worker.close()
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.assertEqual(['404'], client.reply)
        pass

    def test_async_client_timeout_for_hanging_worker(self):
        """
        AsyncClient must timeout when worker was hanging or crashed.
        """
        broker = Broker(self.zmq_ctx)
        worker = Worker(self.zmq_ctx, broker.address[0], 'dummy')
        client = AsyncServiceCaller(self.zmq_ctx, broker.address[0], b'', 'dummy')


        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 4.0, ioloop.stop)

        ioloop.start()
        self.assertTrue(isinstance(client.error_code, OSError))
        self.assertEqual(errno.ETIME, client.error_code.errno)
        self.assertEqual('Request timeout', client.error_code.strerror)

        broker.close()
        worker.close()
        client.close()
        pass

    def test_client_timeout_for_hanging_worker(self):
        """
        Client must timeout when worker was hanging or crashed.
        """
        broker = Broker(self.zmq_ctx)
        worker = Worker(self.zmq_ctx, broker.address[0], 'dummy')
        client = ServiceCaller(self.zmq_ctx, broker.address[0], b'', 'dummy')

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 7.2, ioloop.stop)

        proc = Thread(target=client)
        proc.start()

        ioloop.start()
        proc.join()

        self.assertTrue(isinstance(client.exception, OSError))
        self.assertEqual(errno.ETIME, client.exception.errno)
        self.assertEqual('Request timeout', client.exception.strerror)

        broker.close()
        worker.close()
        client.close()
        pass

    @parameterized.expand([
        ('partial_worker_msg', [b'']),
        ('invalid_protocol_delim', [b'', b'!', b'']),
        ('invalid_protocol_name', [b'', b'', b'']),
        ('unknown_command', [b'', consts.worker, b'\x0f']),
        ('short_request', [b'', consts.worker, consts.command.request, b'123']),
        ('unexpected_request_delimiter', [b'', consts.worker, consts.command.request, b'123', '!', 'msg']),
    ])
    def test_worker_error_resistence(self, name, msg):
        broker = MalformBrokerForWorker(self.zmq_ctx, msg)
        client = AsyncServiceCaller(self.zmq_ctx, broker.address[0], 'Hello', 'echo')
        worker = EchoWorker(self.zmq_ctx, broker.address[0])

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

        ioloop.start()

        self.assertEqual(['ECHO', 'Hello'], client.reply)

        broker.close()
        worker.close()
        client.close()
        pass

    @parameterized.expand([
        ('partial_reply', 'Partial reply', [b'']),
        ('mismatched_reply', 'Mismatched reply', [b'!', b'', b'']),
        ('mismatched_reply', 'Mismatched reply', [consts.client, b'!', b'']),
    ])
    def test_client_error_report(self, name, error_msg, msg):
        broker = MalformBrokerForClient(self.zmq_ctx, msg)
        client = ServiceCaller(self.zmq_ctx, broker.address[0], ['Hello', 'world'], 'echo')

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

        proc = Thread(target=client)
        proc.start()

        ioloop.start()

        proc.join()

        self.assertTrue(isinstance(client.exception, MDPError))
        self.assertEqual(error_msg, client.exception.message)
        broker.close()
        client.close()
        pass

    @parameterized.expand([
        ('partial_reply', 'Partial reply', [b'']),
        ('mismatched_reply', 'Mismatched reply', [b'!', b'', b'']),
        ('mismatched_reply', 'Mismatched reply', [consts.client, b'!', b'']),
    ])
    def test_async_client_error_report(self, name, error_msg, msg):
        broker = MalformBrokerForClient(self.zmq_ctx, msg)
        client = AsyncServiceCaller(self.zmq_ctx, broker.address[0], ['Hello', 'world'], 'echo')

        ioloop = IOLoop.instance()
        ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

        ioloop.start()

        self.assertTrue(isinstance(client.error_code, OSError))
        self.assertEqual(errno.EPROTO, client.error_code.errno)
        self.assertEqual(error_msg, client.error_code.strerror)

        broker.close()
        client.close()
        pass

    def test_client_close_no_op(self):
        broker = Broker(self.zmq_ctx)
        client = ServiceCaller(self.zmq_ctx, broker.address[0], 'Hello', 'echo')
        try:
            client.close()
        except:
            self.fail()
        finally:
            broker.close()
        pass

    def test_enable_verbose(self):
        name = 'test-verbose'
        msg = 'message to be logged'
        logger = logging.getLogger(name)
        stream = StringIO()
        hdlr = logging.StreamHandler(stream)

        logger.setLevel(logging.DEBUG)
        logger.addHandler(hdlr)

        verbose(msg, name)
        self.assertIn(msg, stream.getvalue())
        logger.removeHandler(hdlr)
        stream.close()
        pass

    def test_disable_verbose(self):
        name = 'test-verbose'
        msg = 'message to be logged'
        logger = logging.getLogger(name)
        stream = StringIO()
        hdlr = logging.StreamHandler(stream)

        logger.setLevel(logging.INFO)
        logger.addHandler(hdlr)

        verbose(msg, name)
        self.assertNotIn(msg, stream.getvalue())
        logger.removeHandler(hdlr)
        stream.close()
        pass

    # def test_auto_recover_stream_without_on_recover_cb(self):
    #     broker = Broker(self.zmq_ctx)
    #     socket = self.zmq_ctx.socket(zmq.DEALER)
    #     stream = autorecover_stream(socket, broker.address[0], 0.5, 2)

    #     ioloop = IOLoop.instance()
    #     ioloop.add_timeout(ioloop.time() + 1.0, ioloop.stop)

    #     ioloop.start()

    #     broker.close()
    #     stream.close()
    #     pass

if __name__ == '__main__':
    try:
        unittest.main()
    except KeyboardInterrupt:
        print 'Interrupted'
