#!/usr/bin/env python3

import argparse
import pathlib
import struct
import base64
import math
import copy
import csv
from io import StringIO
import functools
from enum import Enum


class LpFlags(Enum):
    LP_UNUSED = 0x0
    LP_NORMAL = 0x1
    LP_REDIRECT = 0x2
    LP_DEAD = 0x3

class PdFlags(Enum):
    PD_UNDEFINED = 0x0
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
        if not self.pd_flags:
            self.pd_flags = [PdFlags.PD_UNDEFINED] 

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
        header_bytes += struct.pack('<H', functools.reduce(
            lambda x, y: x | y, 
                [x.value for x in self.pd_flags]
            )
        )  
        header_bytes += struct.pack('<H', self.pd_lower)
        header_bytes += struct.pack('<H', self.pd_upper)
        header_bytes += struct.pack('<H', self.pd_special)
        # pack pd_pagesize_version
        header_bytes += struct.pack('<H', self.size | self.version.value)
        header_bytes += struct.pack('<I', self.pd_prune_xid)
        
        return header_bytes


class ItemIdData:
    _FIELD_SIZE = 4

    def __init__(self, item_id_bytes):
        _encoded_data = struct.unpack('<I', item_id_bytes)[0]
        # lower 15 bits store the item offset
        self.lp_off = _encoded_data & 0x7fff
        # middle 2 bits store the item flags
        self.lp_flags = LpFlags((_encoded_data & 0x18000) >> 15)
        # upper 15 bits store the item length
        self.lp_len = (_encoded_data & 0xfffe0000) >> 17

    def to_bytes(self):
        item_id = 0
        # upper 15 bits store the item length
        item_id = item_id | self.lp_len << 17
        # middle 2 bits store the item flags
        item_id = item_id | self.lp_flags.value << 15
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
        
        return block_data_bytes

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


class HeapT_InfomaskFlags(Enum):
    # https://github.com/postgres/postgres/blob/d4e66a39eb96ca514e3f49c85cf0b4b6f138854e/src/include/access/htup_details.h#L188
    HEAP_HASNULL = 0x1
    HEAP_HASVARWIDTH = 0x2
    HEAP_HASEXTERNAL = 0x4
    HEAP_HASOID_OLD = 0x8
    HEAP_XMAX_KEYSHR_LOCK = 0x10
    HEAP_COMBOCID = 0x20
    HEAP_XMAX_EXCL_LOCK = 0x40
    HEAP_XMAX_LOCK_ONLY = 0x80
    HEAP_XMIN_COMMITTED = 0x100
    HEAP_XMIN_INVALID = 0x200
    HEAP_XMAX_COMMITTED = 0x400
    HEAP_XMAX_INVALID = 0x800
    HEAP_XMAX_IS_MULTI = 0x1000
    HEAP_UPDATED = 0x2000
    HEAP_MOVED_OFF = 0x4000
    HEAP_MOVED_IN = 0x8000
    
class HeapT_Infomask2Flags(Enum):
    HEAP_KEYS_UPDATED = 0x2000
    HEAP_HOT_UPDATED = 0x4000
    HEAP_ONLY_TUPLE = 0x8000
    
class T_Infomask:
    HEAP_XACT_MASK = 0xFFF0
    HEAP_LOCK_MASK = (HeapT_InfomaskFlags.HEAP_XMAX_EXCL_LOCK.value | HeapT_InfomaskFlags.HEAP_XMAX_EXCL_LOCK.value)
    
    def __init__(self, t_infomask_bytes):
        _t_infomask = struct.unpack('<H', t_infomask_bytes)[0]
        self.flags = [x for x in HeapT_InfomaskFlags if x.value & _t_infomask]
        
    def to_bytes(self):
        if self.flags:
            return struct.pack('<H', functools.reduce(
                lambda x, y: x | y, 
                    [x.value for x in self.flags]
                )
            )
        return struct.pack('<H', 0)
    
class T_Infomask2:
    HEAP_NATTS_MASK = 0x07FF
    HEAP2_XACT_MASK = 0xE000
    
    def __init__(self, t_infomask2_bytes):
        _t_infomask_2 = struct.unpack('<H', t_infomask2_bytes)[0]
        self.flags = [x for x in HeapT_Infomask2Flags if x.value & _t_infomask_2]
        # get number of attributes in the item
        self.natts = _t_infomask_2 & self.HEAP_NATTS_MASK

    def to_bytes(self):
        if self.flags:
            return struct.pack('<H', self.natts | functools.reduce(
                lambda x, y: x | y, 
                    [x.value for x in self.flags]
                )
            )
        return struct.pack('<H', self.natts | 0)


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
        if HeapT_InfomaskFlags.HEAP_HASNULL in self.t_infomask.flags:
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
        
        if HeapT_InfomaskFlags.HEAP_HASNULL in self.t_infomask.flags:
            heap_tuple_header_bytes += self.nullmap.to_bytes(self.nullmap_byte_size, byteorder='little')
        else:
            heap_tuple_header_bytes += b'\x00'

        return heap_tuple_header_bytes


PARSEABLE_TYPES = [
    'oid',
    'int',
    'int2',
    'int4',
    'int8',
    'bool',
    'date',
    'timetz',
    'timestamptz',
    'time',
    'timestamp',
    'serial',
    'serial2',
    'serial4',
    'serial8'
]



class Item:
    def __init__(self, offset, length, filenode_bytes):
        self.header = HeapTupleHeaderData(offset, filenode_bytes)
        self.data = filenode_bytes[offset+self.header.t_hoff:offset+length]

    def to_bytes(self):
        item_bytes = b''
        item_bytes += self.header.to_bytes()
        item_bytes += self.data
        
        return item_bytes


class Page:
    def __init__(self, offset, filenode_bytes):
        self.offset = offset
        # parse page header
        self.header = PageHeaderData(filenode_bytes[offset:offset+PageHeaderData._FIELD_SIZE])
        # parse page entries
        # pointers to page entries are stored in the ItemIdData
        # objects between the header and the pd_lower offsets
        # each 4 bytes represent a separate ItemIdData object
        # ItemIdData pointers end at the start of empty space,
        # indicated by header.pd_lower field
        items_id_data = filenode_bytes[offset+PageHeaderData._FIELD_SIZE:offset + self.header.pd_lower]
        self.item_ids = [ItemIdData(items_id_data[i:i+4]) for i in range (0, len(items_id_data), 4)]
        # iterate over item ids, populate actual items (i.e. rows) in the page
        # item_id.lp_off will point to the HeapTupleHeaderData object of the actual item
        # we will need to parse this object to obtain information about the item and 
        # an offset to the actual data
        # read HeapTupleHeaderData object
        self.items = [Item(offset+x.lp_off, x.lp_len, filenode_bytes) for x in self.item_ids]
        
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
            page_bytes += bytes((math.ceil(len(item_bytes) / 8) * 8) - len(item_bytes)) 
            
        # pad anything that's left with null bytes to match page length
        page_bytes += bytes(self.header.length - len(page_bytes))
        
        return page_bytes    


class TypAlign(Enum):
    CHAR = 'b'
    SHORT = 'h'
    INT = 'i'
    DOUBLE = 'q'

class DataType:
    _INTERNAL_ATTRS = ('tableoid', 'ctid', 'xmin', 'xmax', 'cmin', 'cmax')

    _ALIGNMENT_MAPPING = {
        'c': TypAlign.CHAR,
        's': TypAlign.SHORT,
        'i': TypAlign.INT,
        'd': TypAlign.DOUBLE
    }

    def __init__(self, csv_str):
        self.field_defs = list()

        for field_def in csv.reader(csv_str.split(';')):
            name, _type, _length, _alignment = field_def
            if name not in self._INTERNAL_ATTRS:
                self.field_defs.append({
                    'name': name,
                    'type': _type,
                    'length': int(_length),
                    'alignment': self._ALIGNMENT_MAPPING[_alignment]
                })


class Varlena:
    _VA_HEADER_SIZE = 0
    _VA_MAX_DATA_SIZE = 0

    def __init__(self):
        pass

    def _get_size(self):
        raise NotImplementedError
    
    def _set_size(self, new_size):
        raise NotImplementedError
    
    def to_bytes(self):
        raise NotImplementedError

# TODO: can do better job with classes than this
class Varlena_1B(Varlena):
    _VA_HEADER_SIZE = 1
    _VA_MAX_DATA_SIZE = 126

    def __init__(self, varlena_bytes=None):
        if varlena_bytes:
            self.va_header = struct.unpack('B', varlena_bytes[:1])[0]
            self.value = varlena_bytes[1:self._get_size()]
        else:
            # Set va_header ID of Varlena_1B object
            self.va_header = 0x1
            self.value = b''

    def _get_size(self):
        return (self.va_header >> 1) & 0x7F
    
    def _set_size(self, new_size):
        if new_size >= self._VA_MAX_DATA_SIZE:
            raise ValueError(f'[-] Varlena new length {new_size} is greater than maximum value of {self._VA_MAX_DATA_SIZE} bytes')
        # account for the size of va_header by adding extra 1 byte
        self.va_header |= ((new_size + 1) & 0x7F) << 1

    def to_bytes(self):
        return struct.pack('B', self.va_header) + self.value

class Varlena_4B(Varlena):
    _VA_HEADER_SIZE = 4

    def __init__(self, varlena_bytes=None):
        if varlena_bytes:
            self.va_header = struct.unpack('<I', varlena_bytes[:4])[0]
            self.value = varlena_bytes[4:self._get_size()]
        else:
            # Set va_header ID of Varlena_4B_U object
            self.va_header = 0x0
            self.value = b''

    def _get_size(self):
        return (self.va_header >> 2) & 0x3FFFFFFF
    
    def _set_size(self, new_size):
        if new_size >= self._VA_MAX_DATA_SIZE:
            raise ValueError(f'[-] Varlena new length {new_size} is greater than maximum value of {self._VA_MAX_DATA_SIZE} bytes')
        # account for the size of va_header by adding extra 4 bytes
        self.va_header |= ((new_size + 4) & 0x3FFFFFFF) << 2

    def to_bytes(self):
        return struct.pack('<I', self.va_header) + self.value

def Path(path):
    path = pathlib.Path(path)
    if not path.exists():
        raise argparse.ArgumentTypeError('Supplied filenode path does not exist')
    if not path.is_file():
        raise argparse.ArgumentTypeError('Supplied filenode path is not a file')
    return path

def Base64Data(b64_data):
    try:
        return base64.b64decode(b64_data)
    except:
        raise argparse.ArgumentTypeError('Invalid base64 data supplied')

def CsvData(csv_data):
    try:
        return list(csv.reader(StringIO(csv_data)))[0]
    except:
        raise argparse.ArgumentTypeError('Invalid CSV data supplied')

def DataTypeCsv(dtype_csv):
    try:
        return DataType(dtype_csv)
    except:
        raise argparse.ArgumentTypeError('Invalid datatype CSV supplied')


parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filenode-path', required=True, type=Path, help='Path to the target PostgreSQL filenode')
parser.add_argument('-m', '--mode', choices=['list', 'read', 'update', 'raw_update'], required=True, help='List items in the target filenode')

parser.add_argument('-p', '--page', type=int, help='Index of the page to read/write')
parser.add_argument('-i', '--item', type=int, help='Index of the item to read/write')

parser.add_argument('-b', '--b64-data', type=Base64Data, help='New item data to set; encoded in Base64; only available in raw_update mode')
parser.add_argument('-c', '--csv-data', type=CsvData, help='New item data to set; encoded in CSV; only available in update mode')
parser.add_argument('-d', '--datatype-csv', type=DataTypeCsv, help='Datatype CSV extracted from the PostgreSQL server')


class Filenode:
    def __init__(self, filenode_path, datatype=None):
        self.datatype = datatype

        with open(filenode_path, 'rb') as f:
            filenode_bytes = f.read()
        
        self.pages = list()
        page_offset = 0
        while page_offset < len(filenode_bytes):
            # parse header bytes of new page
            page = Page(page_offset, filenode_bytes)
            self.pages.append(page)
            page_offset += page.header.length


    def _deserialize_data(self, item_data, item_header):
        if self.datatype is None:
            return item_data

        try:
            unserialized_data = list()
            offset = 0

            # copy of nullmap
            _nullmap = item_header.nullmap

            for i in range(item_header.t_infomask2.natts):
                # check if row has null values
                if HeapT_InfomaskFlags.HEAP_HASNULL in item_header.t_infomask.flags:
                    field_present = _nullmap & 0x1
                    _nullmap >>= 1
                    # if the field is null
                    if not field_present:
                        unserialized_data.append({
                            'name': self.datatype.field_defs[i]['name'],
                            'type': self.datatype.field_defs[i]['type'],
                            'value': b'',
                            'is_null': True
                        })
                        continue

                # handle fixed length fields
                if self.datatype.field_defs[i]['length'] > 0:
                    value = b''
                    length = self.datatype.field_defs[i]['length']
                    
                    if item_data[offset:offset+length]:
                        if self.datatype.field_defs[i]['type'] in PARSEABLE_TYPES:
                            value = struct.unpack(f'<{self.datatype.field_defs[i]["alignment"].value}', item_data[offset:offset+length])[0]
                        else:
                            value = item_data[offset:offset+length]
                
                # handle varlena fields, e.g. text, varchar
                if self.datatype.field_defs[i]['length'] == -1:
                    # varlena struct has a length field at the start
                    # it can either be 1 or 4 bytes

                    # information about the varlena structure is stored in
                    # the first byte

                    # see 
                    # https://doxygen.postgresql.org/varatt_8h_source.html#l00141

                    #TODO: i can do better job with parsing and initiating classes based on value
                    va_header = item_data[offset]
                    # determine type of varlen header
                    if va_header == 0x01:
                        # VARATT_IS_1B_E
                        raise NotImplementedError('Parsing of external varlena structures is not implemented')
                    elif (va_header & 0x01) == 0x01:
                        # VARATT_IS_1B
                        varlena_field = Varlena_1B(item_data[offset:])
                        value = varlena_field.value
                        length = varlena_field._get_size()

                        # Varlena is not padded
                        # if we encounter a Varlena column, and the next
                        # column is not a Varlena, we would need to pad
                        # the data to match the 4 byte alignment
                        if i < item_header.t_infomask2.natts - 1:
                            if self.datatype.field_defs[i+1]['length'] != -1:
                                length += math.ceil((offset+length)/4)*4 - (offset+length)


                    elif (va_header & 0x03) == 0x02:
                        # VARATT_IS_4B_C
                        raise NotImplementedError('Parsing of compressed varlena structures is not implemented')
                    elif (va_header & 0x03) == 0x00:
                        # VARATT_IS_4B_U
                        varlena_field = Varlena_4B(item_data[offset:])
                        value = varlena_field.value
                        length = varlena_field._get_size()

                    else:
                        raise ValueError('Invalid value for Varlena header')
                
                # append the unserialized field to the output
                unserialized_data.append({
                    'name': self.datatype.field_defs[i]['name'],
                    'type': self.datatype.field_defs[i]['type'],
                    'value': value,
                    'is_null': False
                })

                # move past the field we've just read
                offset += length
            return unserialized_data
        except Exception as e:
            print(f'The following exception has occured during deserialization: {e}')

    def _serialize_data(self, item_data, item_header):
        try:
            if self.datatype is None:
                raise Exception('[-] Serialization requires a valid datatype of the filenode')
        
            # if datatype is present, try to serialize the data into bytes
            item_data_bytes = b''
            _nullmap = 0

            for i in range(len(self.datatype.field_defs)):
                field_def = self.datatype.field_defs[i]

                # if field is null, make sure to set HEAP_HASNULL flag in the header
                # skip the processing
                if item_data[i] == 'NULL':
                    item_header.t_infomask.flags = list(set(item_header.t_infomask.flags) + {HeapT_InfomaskFlags.HEAP_HASNULL})
                    _nullmap = (_nullmap | 0) << 1
                    continue
                
                # handle fixed length data fields
                if field_def['length'] > 0:
                    # check if the field type is supported by the parser
                    if field_def['type'] in PARSEABLE_TYPES:
                        item_data_bytes += struct.pack(f'<{field_def["alignment"].value}', int(item_data[i]))
                    # else we would need to set the raw byte value of the field
                    # from the user input
                    else:
                        try:
                            item_data_bytes += base64.b64decode(item_data[i])
                        except:
                            raise NotImplementedError(f'[-] Field {field_def["name"]} has a type {field_def["type"]} that cannot be serialized automatically. Please supply a Base64-encoded byte value in order to edit it.')
                        
                # handle varlena fields
                else:
                    # sanity check, we should be dealing with strings here
                    if not isinstance(item_data[i], str):
                        raise ValueError(f'[-] Field {field_def["name"]} must be a string value!') 
                    # choose correct VarlenA object based on supplied data length
                    if len(item_data[i]) < Varlena_1B._VA_MAX_DATA_SIZE:
                        varlena_field = Varlena_1B()    
                    elif len(item_data[i]) < Varlena_1B._VA_MAX_DATA_SIZE:
                        varlena_field = Varlena_4B()
                    else:
                        raise ValueError(f'[-] Supplied data length is greater than the maximum one of the supported VarlenA structures')
                    # set length and value of the varlena object
                    varlena_field._set_size(len(item_data[i]))
                    varlena_field.value = item_data[i].encode('utf-8')
                    # serialize varlena object to bytes
                    item_data_bytes += varlena_field.to_bytes()

                    # Varlena is not padded
                    # if we encounter a Varlena column, and the next
                    # column is not a Varlena, we would need to pad
                    # the data to match the 4 byte alignment
                    if i < item_header.t_infomask2.natts - 1:
                        if self.datatype.field_defs[i+1]['length'] != -1:
                            item_data_bytes += bytes(math.ceil(len(item_data_bytes)/4)*4 - len(item_data_bytes)) 
                
                # calculate temp nullmap value to set it later in case we encounter
                # any null values
                _nullmap = (_nullmap | 1) << 1

            # if header has HEAP_HASNULL flag present, set nullmap that we calculated
            # earlier
            if HeapT_InfomaskFlags.HEAP_HASNULL in item_header.t_infomask.flags:
                item_header.nullmap = _nullmap

            return item_data_bytes, item_header
        
        except Exception as e:
            print(f'The following exception has occured during deserialization: {e}')


    def list_pages(self):
        for i in range(len(self.pages)):
            print(f'[!] Page {i}:')
            for j in range(len(self.pages[i].items)):
                item = self.pages[i].items[j]
                data = self._deserialize_data(item.data, item.header)
                print(f'[*] Item {j}, length {self.pages[i].item_ids[j].lp_len}:')
                print(f'   {data}')

    def list_page(self, page_id):
        try:
            print(f'[!] Page {page_id}:')
            for j in range(len(self.pages[page_id].items)):
                item = self.pages[page_id].items[j]
                data = self._deserialize_data(item.data, item.header)
                print(f'[*] Item {j}, length {self.pages[page_id].item_ids[j].lp_len}:')
                print(f'   {data}')
        except IndexError:
            print('[-] Non existing page index provided')

    def get_item(self, page_id, item_id):
        try:
            item = self.pages[page_id].items[item_id]
            data = self._deserialize_data(item.data, item.header)
            print(f'[!] Page {page_id}:')
            print(f'[*] Item {item_id}, length {self.pages[page_id].item_ids[item_id].lp_len}:')
            print(f'   {data}')
        except IndexError:
            print('[-] Non existing page or item indexes provided')


    def update_item(self, page_id, item_id, new_item_data):
        try:
            item = self.pages[page_id].items[item_id]
            # check if we the user passed us CSV with datatype, or a raw Base64-encoded data
            if isinstance(new_item_data, list):
                new_item_data, new_item_header = self._serialize_data(new_item_data, item.header)
            else:
                new_item_data = base64.b64decode(new_item_data)
                new_item_header = item.header
            
            # if we update the item with new data that is shorter than
            # the original entry, we can just edit the original data
            
            # if not, we will need to create a new item in the page,
            # indicating the updated row
            
            # if there is no room for the new item in the page, we'd need
            # to either place the new item in the last page, or create a new one
            if len(new_item_data) > len(item.data):
                self._update_item_new_item(page_id, item_id, new_item_data, new_item_header)
            else:    
                self._update_item_inline(page_id, item_id, new_item_data, new_item_header)
            
        except IndexError:
            print('[-] Non existing page or item indexes provided')
        except NotImplementedError:
            print('[-] Setting item data with length greater than the old one is not implemented yet')

    def _update_item_inline(self, page_id, item_id, new_item_data, new_item_header):
        # set new item length in corresponding ItemId object
        self.pages[page_id].item_ids[item_id].lp_len = len(new_item_data) + HeapTupleHeaderData._FIELD_SIZE + self.pages[page_id].items[item_id].header.nullmap_byte_size
        # set new header in the item object
        self.pages[page_id].items[item_id].header = new_item_header
        # set new data in the item object
        self.pages[page_id].items[item_id].data = new_item_data

    def _update_item_new_item(self, page_id, item_id, new_item_data, new_item_header):
        target_item = self.pages[page_id].items[item_id]
        target_item_id = self.pages[page_id].item_ids[item_id]
        # make deep copies of the target Item and ItemId objects
        new_item = copy.deepcopy(target_item)
        new_item_id = copy.deepcopy(self.pages[page_id].item_ids[item_id])
        
        # set new data in the item object
        new_item.data = new_item_data
        # set new header in the item object
        new_item.header = new_item_header
        
        # set corresponding flags in infomask to indicate that the new item 
        # is the updated version of the target item
        new_item.header.t_infomask.flags = list(set(new_item.header.t_infomask.flags + [HeapT_InfomaskFlags.HEAP_XMAX_INVALID, HeapT_InfomaskFlags.HEAP_UPDATED]))
        
        # set the corresponding flags in infomask to indicate that the old
        # item has been updated with the new one
        target_item.header.t_infomask.flags = list(set(target_item.header.t_infomask2.flags) - {HeapT_InfomaskFlags.HEAP_UPDATED, HeapT_InfomaskFlags.HEAP_XMAX_INVALID})
        target_item.header.t_infomask2.flags = list(set(target_item.header.t_infomask2.flags + [HeapT_Infomask2Flags.HEAP_HOT_UPDATED]))
        
        # set xmin and xmax in the old item to be 1 less than the current one to
        # hopefully mark it as "stale"
        target_item.header.t_xmax = target_item.header.t_xmin
        target_item.header.t_xmin -= 1
        # set lp_flags in associated ItemIdData object to LP_DEAD to
        # hopefully mark original entry as "stale"
        target_item_id.lp_flags = LpFlags.LP_DEAD

        new_item.header.t_xmin = target_item.header.t_xmax
        new_item.header.t_xmax = 0
        
        # we have fully prepared to insert the new item into the page
        # we must now calculate the length of the new item, check if
        # there is enough page for the insertion, and compute all necessary
        # offsets
       
        # calculate the alligned byte length of the new item
        new_item_byte_length = math.ceil(len(new_item.to_bytes())/ 8) * 8
        # set byte length in the corresponding ItemIdData object
        new_item_id.lp_len = new_item_byte_length

        if new_item_byte_length > (self.pages[page_id].header.pd_upper - self.pages[page_id].header.pd_lower):
            # create new page with item in it
            self._update_item_new_page(page_id, new_item_id, new_item)
        else:
            # adjust empty space offsets in page header
            # shift pd_lower up 4 bytes due to the new ItemId object being added
            # shift pd_upper down by the length of the new item
            self.pages[page_id].header.pd_lower += 4
            self.pages[page_id].header.pd_upper -= new_item_byte_length
            # adjust offset in the ItemIdData object
            # new item will start at the pd_upper now
            new_item_id.lp_off = self.pages[page_id].header.pd_upper
            # append new data to the list
            self.pages[page_id].item_ids.append(new_item_id)
            self.pages[page_id].items.append(new_item)
            


    def _update_item_new_page(self, page_id, new_item_id, new_item):
        source_page = self.pages[page_id]
        # make copy of the target page
        new_page = copy.deepcopy(source_page)
        # unset any undesired flags
        new_page.header.flags = [PdFlags.PD_UNDEFINED]

        # calculate byte length of the new item and set pd_lower and pd_upper accordingly
        new_item_byte_length = math.ceil(len(new_item.to_bytes())/ 8) * 8
        new_page.header.pd_lower = PageHeaderData._FIELD_SIZE + 4
        new_page.header.pd_upper = new_page.header.length - new_item_byte_length

        # adjust offset in the ItemIdData object
        new_item_id.lp_off = new_page.header.pd_upper
        
        # all is done! we can now insert ItemIdData and Item into the page
        new_page.item_ids = [new_item_id]
        new_page.items = [new_item]

        # append new page to the filenode
        self.pages.append(new_page)
    

    def save_to_path(self, new_filenode_path):
        filenode_bytes = b''.join(x.to_bytes() for x in self.pages)
        
        with open(new_filenode_path, 'wb') as f:
            f.write(filenode_bytes)


if __name__ == '__main__':
    args = parser.parse_args()
    filenode = Filenode(args.filenode_path, args.datatype_csv)
    
    if args.mode == 'list':
        if args.page is not None:
            filenode.list_page(args.page)
        else:
            filenode.list_pages()
    if args.mode == 'read':
        if args.page is not None and args.item is not None:
            filenode.get_item(args.page, args.item)
        else:
            print('[-] please provide page and item indexes via --page and --item arguments')    
    if args.mode == 'raw_update':
        if args.page is not None and args.item is not None and args.b64_data is not None:
            filenode.update_item(args.page, args.item, args.b64_data)
            filenode.save_to_path(args.filenode_path.with_suffix('.new'))
        else:
            print('[-] please provide page, item indexes, and new item data via the --page, --item, and --b64-data arguments')
    if args.mode == 'update':
        if args.page is not None and args.item is not None and args.csv_data is not None and args.datatype_csv is not None:
            filenode.update_item(args.page, args.item, args.csv_data)
            filenode.save_to_path(args.filenode_path.with_suffix('.new'))
        else:
            print('[-] please provide page, item indexes, and new item data via the --page, --item, --datatype-csv and --csv-data arguments')