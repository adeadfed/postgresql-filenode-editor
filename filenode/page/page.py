import math
from .page_header_data import PageHeaderData
from .item_id_data import ItemIdData
from filenode.item import Item


class Page:
    def __init__(self, offset, filenode_bytes):
        self.offset = offset
        # parse page header
        self.header = PageHeaderData(
            filenode_bytes[offset:offset+PageHeaderData._FIELD_SIZE])
        # parse page entries
        # pointers to page entries are stored in the ItemIdData
        # objects between the header and the pd_lower offsets
        # each 4 bytes represent a separate ItemIdData object
        # ItemIdData pointers end at the start of empty space,
        # indicated by header.pd_lower field
        items_id_data = filenode_bytes[
            offset +
            PageHeaderData._FIELD_SIZE:offset +
            self.header.pd_lower
        ]
        self.item_ids = [ItemIdData(items_id_data[i:i+4])
                         for i in range(0, len(items_id_data), 4)]
        # iterate over item ids, populate actual items (i.e. rows) in the page
        # item_id.lp_off will point to the HeapTupleHeaderData object of the
        # actual item we will need to parse this object to obtain information
        # about the item and an offset to the actual data
        # read HeapTupleHeaderData object
        self.items = [Item(offset+x.lp_off, x.lp_len, filenode_bytes)
                      for x in self.item_ids]

    def to_bytes(self):
        page_bytes = b''
        # pack page header
        page_bytes += self.header.to_bytes()
        # pack ItemIdData pointers
        page_bytes += b''.join(x.to_bytes() for x in self.item_ids)
        # pad empty space with null bytes
        # empty space starts at the end of ItemItData pointers
        # indicated by header.pd_lower value
        # and ends at the start of the item entries
        # indicated by header.pd_upper value
        page_bytes += bytes(self.header.pd_upper - self.header.pd_lower)
        # pack page items
        # items must be reversed in order
        items_rev = list(reversed(self.items))
        for i in range(len(items_rev)):
            item_bytes = items_rev[i].to_bytes()
            page_bytes += item_bytes
            # pad the item with null bytes at the end to match
            # the 8 byte data allignment scheme
            page_bytes += bytes((math.ceil(len(item_bytes) / 8)
                                * 8) - len(item_bytes))

        # pad anything that's left with null bytes to match page length
        page_bytes += bytes(self.header.length - len(page_bytes))

        return page_bytes
