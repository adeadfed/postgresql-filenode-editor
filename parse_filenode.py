import argparse
import pathlib
import struct
import functools
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
    
        _pd_flags = struct.unpack('<H', header_bytes[10:12])[0]
        # parse pd_flags raw value into flags
        self.pd_flags = [x for x in PdFlags if x.value & _pd_flags]

        self.pd_lower = struct.unpack('<H', header_bytes[12:14])[0]
        self.pd_upper = struct.unpack('<H', header_bytes[14:16])[0]
        self.pd_special = struct.unpack('<H', header_bytes[16:18])[0]
        
        _pd_pagesize_version = struct.unpack('<H', header_bytes[18:20])[0]
         # parse pd_pagesize_version
        self.size = self.length = _pd_pagesize_version & 0xFF00
        self.version = PdPageVersion(_pd_pagesize_version & 0x00FF)

        self.pd_prune_xid = struct.unpack('<I', header_bytes[20:24])[0]
        
    def to_bytes(self):
        header_bytes = b''
        
        header_bytes += struct.pack('<Q', self.pd_lsn)
        # zero out checksum, just to be super safe with editing data
        header_bytes += struct.pack('<H', 0)
        # pack pd_flags into 32 bit integer via bitwise or
        header_bytes += struct.pack('<H', functools.reduce(lambda x, y: x.value | y.value, self.pd_flags))  
        header_bytes += struct.pack('<H', self.pd_lower)
        header_bytes += struct.pack('<H', self.pd_upper)
        header_bytes += struct.pack('<H', self.pd_special)
        header_bytes += struct.pack('<H', self.pd_pagesize_version)
        header_bytes += struct.pack('<I', self.pd_prune_xid)
        
        return header_bytes


class ItemIdData:
    _FIELD_SIZE = 4

    def __init__(self, item_id_bytes):
        _encoded_data = struct.unpack('<I', item_id_bytes)[0]
        # lower 15 bits store the item offset
        self.lp_off = _encoded_data & 0x7fff
        # middle 2 bits store the item flags
        self.lp_flags = _encoded_data & 0x18000 >> 15
        # upper 15 bits store the item length
        self.lp_len = (_encoded_data & 0xfffe0000) >> 17

    def to_bytes(self):
        item_id = 0
        # upper 15 bits store the item length
        item_id = (item_id | self.lp_len) << 15
        # middle 2 bits store the item flags
        item_id = (item_id | self.lp_flags) << 2
        # lower 15 bits store the item offset
        item_id = item_id | self.lp_off
        return struct.pack('<I', item_id)

class BlockIdData:
    _FIELD_SIZE = 4

    def __init__(self, block_data_bytes):
        self.bi_hi = struct.unpack('<H', block_data_bytes[:2])[0]
        self.bi_lo = struct.unpack('<H', block_data_bytes[2:4])[0]
        
    def to_bytes(self):
        block_data_bytes = b''
        block_data_bytes += struct.pack('<H', self.bi_hi)
        block_data_bytes += struct.pack('<H', self.bi_lo)

class ItemPointerData:
    _FIELD_SIZE = 6

    def __init__(self, item_pointer_bytes):
        self.ip_blkid = BlockIdData(item_pointer_bytes[:4])
        self.ip_posid = struct.unpack('<H', item_pointer_bytes[4:6])[0]

    def to_bytes(self):
        item_pointer_bytes = b''
        item_pointer_bytes += self.ip_blkid.to_bytes()
        item_pointer_bytes += struct.pack('<H', self.ip_posid)


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

    def to_bytes(self):
        heap_tuple_header_bytes = b''
        heap_tuple_header_bytes += struct.pack('<I', self.t_xmin)
        heap_tuple_header_bytes += struct.pack('<I', self.t_xmax)
        heap_tuple_header_bytes += struct.pack('<I', self.t_cid)
        heap_tuple_header_bytes += self.t_ctid.to_bytes()
        heap_tuple_header_bytes += struct.pack('<H', self.t_infomask2)
        heap_tuple_header_bytes += struct.pack('<H', self.t_infomask)
        heap_tuple_header_bytes += struct.pack('B', self.t_hoff)
        
        return heap_tuple_header_bytes

class Item:
    def __init__(self, offset, length, filenode_bytes):
        self.header = HeapTupleHeaderData(filenode_bytes[offset:offset+HeapTupleHeaderData._FIELD_SIZE])
        self.data = filenode_bytes[offset+self.header.t_hoff:offset+length]


class Page:
    def __init__(self, offset, filenode_bytes):
        self.offset = offset
        # parse page header
        self.header = PageHeaderData(filenode_bytes[offset:offset+PageHeaderData._FIELD_SIZE])
        # parse page entries
        # pointers to page entries are stored in the ItemIdData
        # objects between the header and the pd_lower offsets
        # each 4 bytes represent a separate ItemIdData object
        items_id_data = filenode_bytes[offset+PageHeaderData._FIELD_SIZE:offset + self.header.pd_lower]
        self.item_ids = [ItemIdData(items_id_data[i:i+4]) for i in range (0, len(items_id_data), 4)]
        # iterate over item ids, populate actual items (i.e. rows) in the page
        # item_id.lp_off will point to the HeapTupleHeaderData object of the actual item
        # we will need to parse this object to obtain information about the item and 
        # an offset to the actual data
        # read HeapTupleHeaderData object
        self.items = [Item(offset+x.lp_off, x.lp_len, filenode_bytes) for x in self.item_ids]
        
        

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filenode-path', required=True, type=FilenodePath, help='Path to the target PostgreSQL filenode')
parser.add_argument('-m', '--mode', choices=['list', 'read'], help='List items in the target filenode')

parser.add_argument('-p', '--page', type=int, help='Index of the page to read/write')
parser.add_argument('-i', '--item', type=int, help='Index of the item to read/write')


class Filenode:
    def __init__(self, filenode_path):
        with open(filenode_path, 'rb') as f:
            filenode_bytes = f.read()
        
        self.pages = list()
        page_offset = 0
        while page_offset < len(filenode_bytes):
            # parse header bytes of new page
            page = Page(page_offset, filenode_bytes)
            self.pages.append(page)
            page_offset += page.header.length

    def list_pages(self):
        for i in range(len(self.pages)):
            print(f'[!] Page {i}:')
            for j in range(len(self.pages[i].items)):
                print(f'[*] Item {j}, length {self.pages[i].item_ids[j].lp_len}:')
                print(f'   {self.pages[i].items[j].data}')

    def list_page(self, page_id):
        try:
            print(f'[!] Page {page_id}:')
            for j in range(len(self.pages[page_id].items)):
                print(f'[*] Item {j}, length {self.pages[page_id].item_ids[j].lp_len}:')
                print(f'   {self.pages[page_id].items[j].data}')
        except IndexError:
            print('[-] Non existing page index provided')

    def get_item(self, page_id, item_id):
        try:
            print(f'[!] Page {page_id}:')
            print(f'[*] Item {item_id}, length {self.pages[page_id].item_ids[item_id].lp_len}:')
            print(f'   {self.pages[page_id].items[item_id].data}')
        except IndexError:
            print('[-] Non existing page or item indexes provided')

    def update_item(self, page_id, item_id, item_data):
        try:
            old_data_len = len(self.pages[page_id].items[item_id].data)
            if old_data_len < len(item_data):
                raise NotImplementedError
            
            # set new item length in corresponding ItemId object
            self.pages[page_id].item_ids[item_id].lp_len = len(item_data)
            
            # pad the new item data with zeroes to match the old data length
            # in order to avoid breaking the offsets and item allignment
            item_data += bytes(old_data_len - len(item_data))
            
            # set new data in the item object
            self.pages[page_id].items[item_id].data = item_data
        except IndexError:
            print('[-] Non existing page or item indexes provided')
        except NotImplementedError:
            print('[-] Setting item data with length greater than the old one is not implemented yet')





if __name__ == '__main__':
    args = parser.parse_args()
    filenode = Filenode(args.filenode_path)
    
    if args.mode == 'list':
        if args.page:
            filenode.list_page(args.page)
        else:
            filenode.list_pages()
        
    if args.mode == 'read':
        if args.page and args.item:
            filenode.get_item(args.page, args.item)
        else:
            print('[-] please provide page and item indexes via --page and --item arguments')
        
            