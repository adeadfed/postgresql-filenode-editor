class Varlena:
    _VA_DEFAULT_HEADER = 0
    _VA_HEADER_SIZE = 0
    _VA_MAX_DATA_SIZE = 0
    _VA_SIZE_SHIFT = 0
    _VA_SIZE_MASK = 0

    def __init__(self, raw_bytes=None):
        if raw_bytes:
            self.__va_header = int.from_bytes(
                raw_bytes[:self._VA_HEADER_SIZE], 'big')
            self.value = raw_bytes[self._VA_HEADER_SIZE:self.size]
        else:
            # Set va_header ID of Varlena object
            self.__va_header = self._VA_DEFAULT_HEADER
            self.value = b''

    @property
    def size(self):
        return (self.__va_header >> self._VA_SIZE_SHIFT) & self._VA_SIZE_MASK

    @size.setter
    def size(self, new_size):
        if new_size >= self._VA_MAX_DATA_SIZE:
            raise ValueError(
                f'Varlena new length {new_size} is greater than maximum value \
                    of {self._VA_MAX_DATA_SIZE} bytes')
        # account for the size of va_header by adding extra 1 byte
        self.__va_header |= ((new_size + self._VA_HEADER_SIZE)
                             & self._VA_SIZE_MASK) << self._VA_SIZE_SHIFT

    def set_value(self, new_value):
        self.size = len(new_value)
        self.value = new_value

    def to_bytes(self):
        return int.to_bytes(
            self.__va_header,
            length=self._VA_HEADER_SIZE,
            byteorder='little'
        ) + self.value


class Varlena_1B(Varlena):
    _VA_DEFAULT_HEADER = 1
    _VA_HEADER_SIZE = 1
    _VA_MAX_DATA_SIZE = 0x7E
    _VA_SIZE_SHIFT = 1
    _VA_SIZE_MASK = 0x7F


class Varlena_4B(Varlena):
    _VA_DEFAULT_HEADER = 0
    _VA_HEADER_SIZE = 4
    _VA_MAX_DATA_SIZE = 0x40000000
    _VA_SIZE_SHIFT = 2
    _VA_SIZE_MASK = 0x3FFFFFFF
