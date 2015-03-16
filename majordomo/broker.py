import zmq
import logging
from binascii import hexlify
from mdp import constants as consts
from mdp import MDPError
from stream import heartbeat_stream
from cStringIO import StringIO
from utils import verbose


class service_handle(object):
    name = None
    requests = None
    waiting = None

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.waiting = []

    def __str__(self):
        fmt = 'name: {}, request_cnt: {}, waiting: [{}]'
        workers = ', '.join(map(lambda w: w.identity, self.waiting))
        return fmt.format(self.name, len(self.requests), workers)


class worker_handle(object):
    identity = None
    address = None
    service = None

    def __init__(self, identity, address, liveness):
        self.identity = identity
        self.address = address
        self.curr_liveness = self.init_liveness = liveness
        pass

    def __str__(self):
        fmt = 'id: {}, service: {}, liveness: {}'

        class dummy_service(object):
            name = None

        return fmt.format(self.identity,
                          (self.service or dummy_service).name,
                          self.curr_liveness)

    def reset_liveness(self):
        self.curr_liveness = self.init_liveness


class Broker(object):
    stream = None
    services = None   # Lookup registered/requested services
    workers = None    # Quick lookup registered workers
    waiting = None    # LRU queue of waiting workers
    address = None

    def __init__(self, zmq_ctx, heartbeat_interval=2.5, worker_liveness=3,
                 bind_address='tcp://127.0.0.1:7878'):
        self.zmq_ctx = zmq_ctx
        # TODO weakref dict
        self.services = {}
        self.workers = {}
        self.waiting = []
        self.address = []
        self.worker_liveness = worker_liveness
        socket = self.zmq_ctx.socket(zmq.ROUTER)
        self.stream = heartbeat_stream(socket)
        self.bind(bind_address)
        self.stream.on_heartbeat(self.on_heartbeat_, heartbeat_interval)
        self.stream.on_recv(self.dispatch)

        log_fmt = "%(asctime)s %(name)s %(levelname)s %(message)s"
        logging.basicConfig(format=log_fmt,
                            datefmt="%Y-%m-%d %H:%M:%S",
                            level=logging.INFO)

    def __str__(self):
        output = StringIO()
        print >>output, '-' * 10
        print >>output, 'Services:'
        for _, s in self.services.items():
            print >>output, '\t', str(s)
        print >>output, 'Workers:'
        for _, w in self.workers.items():
            print >>output, '\t', str(w)
        print >>output, 'Waiting (workers):'
        for w in self.waiting:
            print >>output, '\t', w.identity
        result = output.getvalue()
        output.close()
        return result

    def bind(self, address):
        self.address.append(address)
        self.stream.bind(address)

    def on_heartbeat_(self):
        def reducer(accu, worker):
            self.stream.snd_heartbeat(worker.address)
            worker.curr_liveness -= 1
            if worker.curr_liveness < 0:
                self.worker_retire(worker, disconnect=False)
            else:
                accu.append(worker)
            return accu
        self.waiting = reduce(reducer, self.waiting, [])
        pass

    def dispatch(self, msg):
        verbose(msg)
        try:
            if len(msg) < 3:
                raise MDPError('Incomplete message')
            address, _, protocol = msg[:3]
            del msg[:3]
            if protocol == consts.client:
                self.process_client(address, msg)
            elif protocol == consts.worker:
                self.process_worker(address, msg)
            else:
                raise MDPError('Unknown protocol')
        except MDPError as e:
            logging.getLogger('mdp').exception(e)
            pass
        pass

    def process_client(self, addr, msg):
        if len(msg) < 2:
            raise MDPError('Invalid client message')
        service = msg.pop(0)
        if service.startswith(consts.internal_service):
            self.mmi_service(addr, service, msg)
        else:
            # XXX costy
            self.mediate(self.require_service(service), [addr, ''] + msg)
        pass

    def process_worker(self, addr, msg):
        if len(msg) < 1:
            raise MDPError('Invalid worker message')
        command = msg.pop(0)
        has_worker = hexlify(addr) in self.workers
        worker = self.require_worker(addr)

        if consts.command.ready == command:
            service = msg.pop(0)
            if has_worker or service.startswith(consts.internal_service):
                self.worker_retire(worker, disconnect=True)
                return
            worker.service = self.require_service(service)
            self.worker_onboard(worker)
            self.mediate(worker.service)
        elif consts.command.reply == command:
            if not has_worker:
                self.worker_retire(worker, disconnect=True)
                return
            addr, _ = msg[:2]
            del msg[:2]
            self.stream.snd_to_client(addr, worker.service.name, msg)
            self.worker_onboard(worker)
            self.mediate(worker.service)
        elif consts.command.heartbeat == command:
            if has_worker:
                worker.reset_liveness()
            else:
                self.worker_retire(worker, disconnect=True)
        elif consts.command.disconnect == command:
            self.worker_retire(worker, disconnect=False)
        else:
            raise MDPError('Invalid worker command')
        pass

    def mmi_service(self, addr, service, msg):
        code = '501'
        if service == 'mmi.service':
            name = msg[-1]
            code = '200' if name in self.services else '404'
        self.stream.snd_to_client(addr, service, code)

    def require_worker(self, addr):
        identity = hexlify(addr)
        worker = self.workers.get(identity)
        if worker is None:
            worker = worker_handle(identity, addr, self.worker_liveness)
            self.workers[identity] = worker
        return worker

    def require_service(self, name):
        assert name is not None
        service = self.services.get(name)
        if service is None:
            service = service_handle(name)
            self.services[name] = service
        return service

    def worker_onboard(self, worker):
        self.waiting.append(worker)
        worker.service.waiting.append(worker)

    def worker_retire(self, worker, disconnect):
        if disconnect:
            self.stream.snd_disconnect(worker.address)
        if worker.service is not None:
            worker.service.waiting.remove(worker)
        self.workers.pop(worker.identity)
        try:
            self.waiting.remove(worker)
        except ValueError:
            pass
        pass

    def mediate(self, service, msg=None):
        assert isinstance(service, service_handle)
        if msg:
            service.requests.append(msg)
        self.on_heartbeat_()
        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            self.stream.snd_request(worker.address, service.name, msg)
            pass
        pass

    def close(self):
        self.stream.close()
