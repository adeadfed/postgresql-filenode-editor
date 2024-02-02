import math
import copy
import base64
import struct
from loguru import logger
from prettytable import PrettyTable

from .page import Page
from .page.item_id_data import LpFlags
from .page.page_header_data import PageHeaderData, PdFlags

from .item.t_infomask import HeapT_InfomaskFlags, HeapT_Infomask2Flags
from .item.heap_tuple_header_data import HeapTupleHeaderData

from pg_types import PARSEABLE_TYPES, Varlena_1B, Varlena_4B, DataType, \
      DataTypeRaw


class Filenode:
    def __init__(self, filenode_path, datatype=None):
        self.datatype = datatype if datatype is not None else DataTypeRaw()

        with open(filenode_path, 'rb') as f:
            filenode_bytes = f.read()

        self.pages = list()
        page_offset = 0
        while page_offset < len(filenode_bytes):
            # parse header bytes of new page
            page = Page(filenode_bytes, page_offset)
            self.pages.append(page)
            page_offset += page.header.length

    def _apply_item_nullmap_to_datatype_from_header(self, item_header):
        item_datatype = list()
        # if there is a nullmap present, process it
        if HeapT_InfomaskFlags.HEAP_HASNULL in HeapT_InfomaskFlags(
                item_header.t_infomask.flags
        ):
            # copy of nullmap
            _nullmap = item_header.nullmap
            for field_def in self.datatype.field_defs:
                # check if row has null values
                field_present = _nullmap & 0x1
                _nullmap >>= 1

                item_datatype.append(
                    field_def | {'is_null': not field_present}
                )
        # else just set all fields to not null
        else:
            for field_def in self.datatype.field_defs:
                item_datatype.append(
                    field_def | {'is_null': False}
                )
        return item_datatype

    def _apply_item_nullmap_to_datatype_from_data(self, item_data):
        item_datatype = list()

        for field_def, item_data in zip(self.datatype.field_defs, item_data):
            # check if row has null values
            item_datatype.append(
                field_def | {'is_null': item_data == 'NULL'}
            )
        return item_datatype

    def _deserialize_data_datatype(self, item_data, item_header):
        deserialized_data = list()
        offset = 0

        item_datatype = self._apply_item_nullmap_to_datatype_from_header(
            item_header)

        for i in range(len(item_datatype)):
            field_def = item_datatype[i]
            # handle null fields
            if field_def['is_null']:
                length = 0
                value = b''

            # handle fixed length fields
            elif field_def['length'] > 0:
                length = field_def['length']
                value = self._deserialize_fixed_len_field(
                    field_def,
                    item_data[offset:offset+length]
                )

            # handle varlena fields, e.g. text, varchar
            elif field_def['length'] == -1:
                varlena_field = self._deserialize_varlena_field(
                    item_data[offset:]
                )

                value = varlena_field.value
                length = varlena_field.size

                # Varlena_1B is not padded
                # if we encounter a Varlena_1B column, and the next
                # column is not a Varlena, and is not null, we would
                # need to pad the data to match the 4 byte alignment
                if i + 1 < len(item_datatype):
                    if all([
                        isinstance(varlena_field, Varlena_1B),
                        item_datatype[i+1]['length'] != -1,
                        not item_datatype[i+1]['is_null']
                    ]):
                        length += math.ceil((offset+length)/4) * \
                            4 - (offset+length)

            else:
                raise Exception('the field is of neither fixed nor \
                                variable length')

            # append the unserialized field to the output
            deserialized_data.append({
                'name': field_def['name'],
                'type': field_def['type'],
                'value': value,
                'is_null': field_def['is_null']
            })

            # move past the field we've just read
            offset += length
        return deserialized_data

    def _deserialize_data_raw(self, item_data):
        # if we are dealing with DataTypeRaw object, just return the data
        return [{
            'name': 'raw_data',
            'type': '',
            'value': item_data,
            'is_null': False
        }]

    def _deserialize_data(self, item_data, item_header):
        try:
            if isinstance(self.datatype, DataTypeRaw):
                return self._deserialize_data_raw(item_data)
            return self._deserialize_data_datatype(item_data, item_header)
        except Exception:
            logger.exception('An exception occured during deserialization')

    def _deserialize_fixed_len_field(self, field_def, field_bytes):
        if field_bytes:
            if field_def['type'] in PARSEABLE_TYPES:
                return struct.unpack(
                    f'<{field_def["alignment"]}',
                    field_bytes
                )[0]
        # not supported fixed length type or empty data
        # just return the byteslice back
        return field_bytes

    def _deserialize_varlena_field(self, field_bytes):
        # varlena struct has a length field at the start
        # it can either be 1 or 4 bytes

        # information about the varlena structure is stored in
        # the first byte

        # see
        # https://doxygen.postgresql.org/varatt_8h_source.html#l00141
        va_header = field_bytes[0]
        # determine type of varlen header
        if va_header == 0x01:
            # VARATT_IS_1B_E
            raise NotImplementedError(
                'Parsing of external varlena structures is not \
                    implemented')
        elif (va_header & 0x01) == 0x01:
            # VARATT_IS_1B
            return Varlena_1B(field_bytes)
        elif (va_header & 0x03) == 0x02:
            # VARATT_IS_4B_C
            raise NotImplementedError(
                'Parsing of compressed varlena structures is not \
                    implemented')
        elif (va_header & 0x03) == 0x00:
            # VARATT_IS_4B_U
            return Varlena_4B(field_bytes)
        raise ValueError('Invalid value for Varlena header')

    def _serialize_data(self, item_data, item_header):
        try:
            # if datatype is present, try to serialize the data into bytes
            serialized_data = b''

            item_datatype = self._apply_item_nullmap_to_datatype_from_data(
                item_data)

            for i in range(len(item_datatype)):
                field_def = item_datatype[i]
                # handle null fields
                if field_def['is_null']:
                    continue
                # handle fixed length data fields
                elif field_def['length'] > 0:
                    serialized_data += self._serialize_fixed_len_field(
                        field_def,
                        item_data[i]
                    )
                # handle varlena fields
                elif field_def['length'] == -1:
                    varlena_field = self._serialize_varlena_field(
                        field_def,
                        item_data[i]
                    )
                    # serialize varlena object to bytes
                    serialized_data += varlena_field.to_bytes()

                    # Varlena_1B is not padded
                    # if we encounter a Varlena_1B column, and the next
                    # column is not a Varlena, and is not null, we would
                    # need to pad the data to match the 4 byte alignment
                    if i + 1 < len(item_datatype):
                        if all([
                            isinstance(varlena_field, Varlena_1B),
                            item_datatype[i+1]['length'] != -1,
                            not item_datatype[i+1]['is_null']
                        ]):
                            serialized_data += bytes(
                                math.ceil(len(serialized_data)/4)*4 -
                                len(serialized_data)
                            )
                else:
                    raise NotImplementedError(
                        'the field is of neither fixed nor variable length'
                    )

            # set nullmap to 0 (default case)
            _nullmap = 0
            # if any of the fields is null, we need to recalculate
            # nullmap value
            if any(f['is_null'] for f in item_datatype):
                # indicate in header that we have a nullmap
                item_header.t_infomask.flags |= \
                    HeapT_InfomaskFlags.HEAP_HASNULL
                # for every field value construct a nullmap
                # NULL field in nullmap will be marked as 0
                # non-null fields will be marked as 1
                # order of fields must be reversed due to
                # little-endian architecture
                # cast resulting nullmap into integer
                # e.g.
                # [1, NULL, Test, NULL, NULL]
                # will produce the following bitmap
                # '00101' or 5
                _nullmap = int(''.join(str(int(not f['is_null']))
                               for f in reversed(item_datatype)), 2)
            else:
                # specifically unset HASNULL flag from header
                item_header.t_infomask.flags &= \
                    ~HeapT_InfomaskFlags.HEAP_HASNULL

            # set nullmap value to header
            item_header.nullmap = _nullmap

            return serialized_data, item_header

        except Exception:
            logger.exception('An exception occured during deserialization')

    def _serialize_fixed_len_field(self, field_def, field_value):
        # check if the field type is supported by the parser
        if field_def['type'] in PARSEABLE_TYPES:
            return struct.pack(
                f'<{field_def["alignment"]}',
                int(field_value)
            )
        # else we would need to set the raw byte value of the field from the
        # user input
        try:
            return base64.b64decode(field_value)
        except Exception:
            raise NotImplementedError(
                f'Field {field_def["name"]} has a type {field_def["type"]}\
                    that cannot be serialized automatically. Please supply a \
                Base64-encoded byte value in order to edit it.')

    def _serialize_varlena_field(self, field_def, field_value):
        # sanity check, we should be dealing with strings here
        if not isinstance(field_value, str):
            raise ValueError(
                f'Field {field_def["name"]} must be a string \
                    value!')
        # choose correct VarlenA object based on supplied data
        # length
        if len(field_value) < Varlena_1B._VA_MAX_DATA_SIZE:
            varlena_field = Varlena_1B()
        elif len(field_value) < Varlena_1B._VA_MAX_DATA_SIZE:
            varlena_field = Varlena_4B()
        else:
            raise ValueError('Data length is greater than the \
                maximum one of the supported VarlenA structures')
        # set value of the varlena object
        varlena_field.set_value(field_value.encode('utf-8'))
        return varlena_field

    def print_data(self, items_to_print):
        # init pretty table object
        page_table = PrettyTable()

        # set field names according to the datatype
        # if it exists
        page_field_names = [x['name'] for x in self.datatype.field_defs]

        page_table.field_names = ['item_no'] + page_field_names

        for i in range(len(items_to_print)):
            table_row = [i]
            # if datatype exists, parse item fields and add them to the row
            table_row += [x['value'] if x['value'] else 'NULL'
                          for x in items_to_print[i]]
            # else, add entire raw item to the row
            page_table.add_row(table_row)

        print(page_table)

    def list_pages(self):
        for i in range(len(self.pages)):
            self.list_page(i)

    def list_page(self, page_id):
        try:
            logger.success(f'Page {page_id}:')
            items_to_print = list()

            for j in range(len(self.pages[page_id].items)):
                item = self.pages[page_id].items[j]
                # deserialize data if datatype is present
                items_to_print.append(
                    self._deserialize_data(item.data, item.header)
                )

            self.print_data(items_to_print)
        except IndexError:
            logger.error('Non existing page index provided')

    def read_item(self, page_id, item_id):
        try:
            item = self.pages[page_id].items[item_id]
            # deserialize data if datatype is present
            data = self._deserialize_data(item.data, item.header)
            logger.success(f'Page {page_id}:')
            self.print_data([data])

            return data
        except IndexError:
            logger.error('Non existing page or item indexes provided')

    def update_item(self, page_id, item_id, new_item_data):
        try:
            item = self.pages[page_id].items[item_id]
            # check if we the user passed us CSV with datatype, or a raw
            # Base64-encoded data
            if isinstance(new_item_data, list):
                # check if user gave us a proper datatype
                if not type(self.datatype) is DataType:
                    raise Exception(
                        'Serialization requires a valid datatype of the \
                            filenode')

                # check if datatype and data have matching number of
                # fields
                if len(self.datatype.field_defs) != len(new_item_data):
                    raise Exception(
                        'Number of supplied values in --data-csv parameter \
                            does not match the number of fields in datatype')
                # try to serialize data into raw bytes using datatype
                new_item_data, new_item_header = self._serialize_data(
                    new_item_data, item.header)
            else:
                # else try to use raw bytes from a user-supplied b64
                # string
                new_item_data = base64.b64decode(new_item_data)
                new_item_header = item.header

            # if we update the item with new data that is shorter than
            # the original entry, we can just edit the original data

            # if not, we will need to create a new item in the page,
            # indicating the updated row

            # if there is no room for the new item in the page, we'd need
            # to either place the new item in the last page, or create
            # a new one
            if len(new_item_data) > len(item.data):
                self._update_item_new_item(
                    page_id, item_id, new_item_data, new_item_header)
            else:
                self._update_item_inline(
                    page_id, item_id, new_item_data, new_item_header)

        except IndexError:
            logger.error('Non existing page or item indexes provided')

    def _update_item_inline(self, page_id, item_id, new_item_data,
                            new_item_header):
        # set new item length in corresponding ItemId object
        self.pages[page_id].item_ids[item_id].lp_len = len(new_item_data) + \
            HeapTupleHeaderData._FIELD_SIZE + \
            self.pages[page_id].items[item_id].header.nullmap_byte_size
        # set new header in the item object
        self.pages[page_id].items[item_id].header = new_item_header
        # set new data in the item object
        self.pages[page_id].items[item_id].data = new_item_data

    def _update_item_new_item(self, page_id, item_id, new_item_data,
                              new_item_header):
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
        new_item.header.t_infomask.flags |= (
            HeapT_InfomaskFlags.HEAP_XMAX_INVALID |
            HeapT_InfomaskFlags.HEAP_UPDATED
        )

        # set the corresponding flags in infomask to ivalue == ndicate that
        # the old item has been updated with the new one
        target_item.header.t_infomask.flags &= ~(
            HeapT_InfomaskFlags.HEAP_UPDATED |
            HeapT_InfomaskFlags.HEAP_XMAX_INVALID
        )
        target_item.header.t_infomask2.flags |= \
            HeapT_Infomask2Flags.HEAP_HOT_UPDATED

        # set xmin and xmax in the old item to be 1 less than the current one
        # to hopefully mark it as "stale"
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
        new_item_byte_length = math.ceil(len(new_item.to_bytes()) / 8) * 8
        # set byte length in the corresponding ItemIdData object
        new_item_id.lp_len = new_item_byte_length

        if new_item_byte_length > (self.pages[page_id].header.pd_upper -
                                   self.pages[page_id].header.pd_lower):
            # create new page with item in it
            self._update_item_new_page(page_id, new_item_id, new_item)
        else:
            # adjust empty space offsets in page header
            # shift pd_lower up 4 bytes due to the new ItemId object
            # being added
            self.pages[page_id].header.pd_lower += 4
            # shift pd_upper down by the length of the new item
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

        # calculate byte length of the new item and set pd_lower and pd_upper
        # accordingly
        new_item_byte_length = math.ceil(len(new_item.to_bytes()) / 8) * 8
        new_page.header.pd_lower = PageHeaderData._FIELD_SIZE + 4
        new_page.header.pd_upper = new_page.header.length - \
            new_item_byte_length

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
