import argparse
import pathlib
import struct
from enum import Enum

def FilenodePath(path):
    filenode_path = pathlib.Path(path)
    if not filenode_path.exists():
        raise argparse.ArgumentTypeError('Supplied filenode path does not exist')
    if not filenode_path.is_file():
        raise argparse.ArgumentTypeError('Supplied filenode path is not a file')
    return filenode_path


class LpFlags(Enum):
    LP_UNUSED = 0x0
    LP_NORMAL = 0x1
    LP_REDIRECT = 0x2
    LP_DEAD = 0x3

class PdFlags(Enum):
    PD_HAS_FREE_LINES = 0x1
    PD_PAGE_FULL = 0x2
    PD_ALL_VISIBLE = 0x4

class PdPageVersion(Enum):
    PRE_POSTGRES_73  = 0x0
    POSTGRES_73_74 = 0x1
    POSTGRES_80 = 0x2
    POSTGRES_81_82 = 0x3
    POSTGRES_83_PLUS = 0x4

class PageHeaderData:
    _FIELD_SIZE = 24 

    def __init__(self, header_bytes):
        # parse raw header bytes
        self.pd_lsn = struct.unpack('<Q', header_bytes[:8])[0]
        self.checksum = struct.unpack('<H', header_bytes[8:10])[0]
        self._pd_flags = struct.unpack('<H', header_bytes[10:12])[0]
        self.pd_lower = struct.unpack('<H', header_bytes[12:14])[0]
        self.pd_upper = struct.unpack('<H', header_bytes[14:16])[0]
        self.pd_special = struct.unpack('<H', header_bytes[16:18])[0]
        self.pd_pagesize_version = struct.unpack('<H', header_bytes[18:20])[0]
        self.pd_prune_xid = struct.unpack('<I', header_bytes[20:24])[0]
        
        # parse pd_flags raw value into flags
        self.pd_flags = [x for x in PdFlags if x.value & self._pd_flags]

        # parse pd_pagesize_version
        self.size = self.length = self.pd_pagesize_version & 0xFF00
        self.version = PdPageVersion(self.pd_pagesize_version & 0x00FF)


class ItemIdData:
    _FIELD_SIZE = 4

    def __init__(self, item_id_bytes):
        _encoded_data = struct.unpack('<I', item_id_bytes)[0]
        self.lp_off = _encoded_data & 0x7fff             # lower 15 bits store the item offset
        self.lp_flags = _encoded_data & 0x18000 >> 15    # middle 2 bits store the item flags
        self.lp_len = (_encoded_data & 0xfffe0000) >> 17 # upper 15 bits store the item length


class BlockIdData:
    _FIELD_SIZE = 4

    def __init__(self, block_data_bytes):
        self.bi_hi = struct.unpack('<H', block_data_bytes[:2])[0]
        self.bi_lo = struct.unpack('<H', block_data_bytes[2:4])[0]

class ItemPointerData:
    _FIELD_SIZE = 6

    def __init__(self, item_pointer_bytes):
        self.ip_blkid = BlockIdData(item_pointer_bytes[:4])
        self.ip_posid = struct.unpack('<H', item_pointer_bytes[4:6])[0]


class HeapTupleHeaderData:
    _FIELD_SIZE = 23

    def __init__(self, heap_tuple_header_bytes):
        # I do not account for DatumTupleFields here
        # https://github.com/postgres/postgres/blob/9d49837d7144e27ad8ea8918acb28f9872cb1585/src/include/access/htup_details.h#L134
        self.t_xmin = struct.unpack('<I', heap_tuple_header_bytes[:4])[0]
        self.t_xmax = struct.unpack('<I', heap_tuple_header_bytes[4:8])[0]
        
        # fields can overlap
        # https://github.com/postgres/postgres/blob/9d49837d7144e27ad8ea8918acb28f9872cb1585/src/include/access/htup_details.h#L122
        self.t_cid = struct.unpack('<I', heap_tuple_header_bytes[8:12])[0]
        self.t_xvac = self.t_cid

        self.t_ctid = ItemPointerData(heap_tuple_header_bytes[12:18])

        self.t_infomask2 = struct.unpack('<H', heap_tuple_header_bytes[18:20])[0]
        self.t_infomask = struct.unpack('<H', heap_tuple_header_bytes[20:22])[0]
        self.t_hoff = struct.unpack('B', heap_tuple_header_bytes[22:23])[0]


class Item:
    def __init__(self, offset, length, filenode):
        self.header = HeapTupleHeaderData(filenode[offset:offset+HeapTupleHeaderData._FIELD_SIZE])
        self.data = filenode[offset+self.header.t_hoff:offset+length]



class Page:
    def __init__(self, offset, filenode):
        self.offset = offset
        # parse page header
        self.header = PageHeaderData(filenode[offset:offset+PageHeaderData._FIELD_SIZE])
        # parse page entries
        # pointers to page entries are stored in the ItemIdData
        # objects between the header and the pd_lower offsets
        # each 4 bytes represent a separate ItemIdData object
        items_id_data = filenode[offset+PageHeaderData._FIELD_SIZE:offset + self.header.pd_lower]
        self.item_ids = [ItemIdData(items_id_data[i:i+4]) for i in range (0, len(items_id_data), 4)]
        
        # iterate over item ids, populate actual items (i.e. rows) in the page
        # item_id.lp_off will point to the HeapTupleHeaderData object of the actual item
        # we will need to parse this object to obtain information about the item and 
        # an offset to the actual data
        # read HeapTupleHeaderData object
        self.items = [Item(offset+x.lp_off, x.lp_len, filenode) for x in self.item_ids]
        
        

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filenode-path', required=True, type=FilenodePath, help='Path to the target PostgreSQL filenode')

def parse_filenode(filenode_path):
    with open(filenode_path, 'rb') as f:
        filenode = f.read()
    
    pages = list()
    page_offset = 0
    while page_offset < len(filenode):
        # parse header bytes of new page
        page = Page(page_offset, filenode)
        pages.append(page)
        page_offset += page.header.length

    for i in range(len(pages)):
        print(f'[*] Page {i}:')
        for j in range(len(pages[i].items)):
            print(f' - Entry {j}: ')
            print(f'   {pages[i].items[j].data}')
   
    

if __name__ == '__main__':
    args = parser.parse_args()
    parse_filenode(args.filenode_path)