
class constants(object):

    client = b'MDPC01'
    worker = b'MDPW01'
    internal_service = b'mmi.'

    class command(object):
        ready       = b'\x01'
        request     = b'\x02'
        reply       = b'\x03'
        heartbeat   = b'\x04'
        disconnect  = b'\x05'
        pass
    pass


class MDPError(RuntimeError):
    pass
