import struct

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
            raise ValueError(f'Varlena new length {new_size} is greater than maximum value of {self._VA_MAX_DATA_SIZE} bytes')
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
            raise ValueError(f'Varlena new length {new_size} is greater than maximum value of {self._VA_MAX_DATA_SIZE} bytes')
        # account for the size of va_header by adding extra 4 bytes
        self.va_header |= ((new_size + 4) & 0x3FFFFFFF) << 2

    def to_bytes(self):
        return struct.pack('<I', self.va_header) + self.value
