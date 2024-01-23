import math
import copy
import base64
import struct

from .page import Page
from .page.item_id_data import LpFlags
from .page.page_header_data import PageHeaderData, PdFlags

from .item.t_infomask import HeapT_InfomaskFlags, HeapT_Infomask2Flags
from .item.heap_tuple_header_data import HeapTupleHeaderData

from pg_types import PARSEABLE_TYPES, Varlena_1B, Varlena_4B


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
                if HeapT_InfomaskFlags.HEAP_HASNULL in HeapT_InfomaskFlags(item_header.t_infomask.flags):
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
                    item_header.t_infomask.flags += HeapT_InfomaskFlags.HEAP_HASNULL
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
            if HeapT_InfomaskFlags.HEAP_HASNULL in HeapT_InfomaskFlags(item_header.t_infomask.flags):
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

            return data
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
        new_item.header.t_infomask.flags += HeapT_InfomaskFlags.HEAP_XMAX_INVALID | HeapT_InfomaskFlags.HEAP_UPDATED
        
        # set the corresponding flags in infomask to indicate that the old
        # item has been updated with the new one
        target_item.header.t_infomask.flags -= HeapT_InfomaskFlags.HEAP_UPDATED | HeapT_InfomaskFlags.HEAP_XMAX_INVALID
        target_item.header.t_infomask2.flags += HeapT_Infomask2Flags.HEAP_HOT_UPDATED
        
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
        new_page.header.flags = PdFlags.PD_UNDEFINED

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

