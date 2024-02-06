import csv


class DataType:
    _INTERNAL_ATTRS = ('tableoid', 'ctid', 'xmin', 'xmax', 'cmin', 'cmax')

    _PG_TO_PY_TYPE_MAPPING = {
        'c': 'b',  # CHAR
        's': 'h',  # SHORT
        'i': 'i',  # INT
        'd': 'q',  # DOUBLE
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
                    'alignment': self._PG_TO_PY_TYPE_MAPPING[_alignment]
                })

    def _field_no_padding(self, field_def):
        return any([
            field_def['alignment'] == 'b',
            field_def['length'] == -1
        ])

    def _get_next_non_null_field(self, datatype):
        for field_def in datatype:
            if not field_def['is_null']:
                return field_def


class DataTypeRaw(DataType):
    def __init__(self):
        self.field_defs = [{
            'name': 'raw_data',
            'type': '',
            'length': '',
            'alignment': ''
        }]
