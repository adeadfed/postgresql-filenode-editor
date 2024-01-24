import struct

from .block_id_data import BlockIdData


class ItemPointerData:
    _FIELD_SIZE = 6

    def __init__(self, item_pointer_bytes):
        self.ip_blkid = BlockIdData(item_pointer_bytes[:4])
        self.ip_posid = struct.unpack('<H', item_pointer_bytes[4:6])[0]

    def to_bytes(self):
        item_pointer_bytes = b''
        item_pointer_bytes += self.ip_blkid.to_bytes()
        item_pointer_bytes += struct.pack('<H', self.ip_posid)

        return item_pointer_bytes
