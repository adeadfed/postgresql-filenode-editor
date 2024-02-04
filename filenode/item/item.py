from .heap_tuple_header_data import HeapTupleHeaderData


class Item:
    def __init__(self, offset, length, filenode_bytes):
        self.header = HeapTupleHeaderData(offset, filenode_bytes)
        self.data = filenode_bytes[offset+self.header.t_hoff:offset+length]

    def to_bytes(self):
        item_bytes = b''
        item_bytes += self.header.to_bytes()
        item_bytes += bytes(self.header.t_hoff - len(self.header.to_bytes()))
        item_bytes += self.data

        return item_bytes
