import math
import struct

from filenode.page.item_pointer_data import ItemPointerData
from .t_infomask import T_Infomask, T_Infomask2, HeapT_InfomaskFlags

class HeapTupleHeaderData:
    _FIELD_SIZE = 23

    def __init__(self, offset, filenode_bytes):
        # I do not account for DatumTupleFields here
        # https://github.com/postgres/postgres/blob/9d49837d7144e27ad8ea8918acb28f9872cb1585/src/include/access/htup_details.h#L134
        self.t_xmin = struct.unpack('<I', filenode_bytes[offset:offset+4])[0]
        self.t_xmax = struct.unpack('<I', filenode_bytes[offset+4:offset+8])[0]
        
        # fields can overlap
        # https://github.com/postgres/postgres/blob/9d49837d7144e27ad8ea8918acb28f9872cb1585/src/include/access/htup_details.h#L122
        self.t_cid = struct.unpack('<I', filenode_bytes[offset+8:offset+12])[0]
        self.t_xvac = self.t_cid

        self.t_ctid = ItemPointerData(filenode_bytes[offset+12:offset+18])

        self.t_infomask2 = T_Infomask2(filenode_bytes[offset+18:offset+20])
        self.t_infomask = T_Infomask(filenode_bytes[offset+20:offset+22])
        self.t_hoff = struct.unpack('B', filenode_bytes[offset+22:offset+23])[0]
        
        self.nullmap_byte_size = 1
        self.nullmap = 0

        # if there is a null map, try to read it now
        if HeapT_InfomaskFlags.HEAP_HASNULL in HeapT_InfomaskFlags(self.t_infomask.flags):
            # null map has the bit size of the attribute number alligned to bytes
            self.nullmap_byte_size = math.ceil(self.t_infomask2.natts / 8)
            self.nullmap = int.from_bytes(filenode_bytes[offset+23:offset+23+self.nullmap_byte_size], byteorder='little')

    def to_bytes(self):
        heap_tuple_header_bytes = b''
        heap_tuple_header_bytes += struct.pack('<I', self.t_xmin)
        heap_tuple_header_bytes += struct.pack('<I', self.t_xmax)
        heap_tuple_header_bytes += struct.pack('<I', self.t_cid)
        heap_tuple_header_bytes += self.t_ctid.to_bytes()
        heap_tuple_header_bytes += self.t_infomask2.to_bytes()
        heap_tuple_header_bytes += self.t_infomask.to_bytes()
        heap_tuple_header_bytes += struct.pack('B', self.t_hoff)
        
        # for some reason this fails without explicit typecast in HeapT_InfomaskFlags enum object
        if HeapT_InfomaskFlags.HEAP_HASNULL in HeapT_InfomaskFlags(self.t_infomask.flags):
            heap_tuple_header_bytes += self.nullmap.to_bytes(self.nullmap_byte_size, byteorder='little')
        else:
            heap_tuple_header_bytes += b'\x00'

        return heap_tuple_header_bytes
