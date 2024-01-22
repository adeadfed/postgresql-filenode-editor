import struct

class BlockIdData:
    _FIELD_SIZE = 4

    def __init__(self, block_data_bytes):
        self.bi_hi = struct.unpack('<H', block_data_bytes[:2])[0]
        self.bi_lo = struct.unpack('<H', block_data_bytes[2:4])[0]
        
    def to_bytes(self):
        block_data_bytes = b''
        block_data_bytes += struct.pack('<H', self.bi_hi)
        block_data_bytes += struct.pack('<H', self.bi_lo)
        
        return block_data_bytes
