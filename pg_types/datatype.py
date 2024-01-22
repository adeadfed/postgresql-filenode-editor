import csv

from .type_alignment import TypAlign


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
