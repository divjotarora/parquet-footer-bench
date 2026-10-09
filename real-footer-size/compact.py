"""Small, general Thrift Compact Protocol reader/writer used by footer_size.py."""

STOP = 0
BOOL_TRUE = 1
BOOL_FALSE = 2
BYTE = 3
I16 = 4
I32 = 5
I64 = 6
DOUBLE = 7
BINARY = 8
LIST = 9
SET = 10
MAP = 11
STRUCT = 12


class DecodeError(ValueError):
    pass


class Reader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def take(self, size):
        end = self.offset + size
        if end > len(self.data):
            raise DecodeError("unexpected end of compact-Thrift input")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def byte(self):
        return self.take(1)[0]

    def varint(self):
        value = 0
        shift = 0
        while shift < 70:
            byte = self.byte()
            value |= (byte & 0x7f) << shift
            if not byte & 0x80:
                return value
            shift += 7
        raise DecodeError("invalid varint")

    def zigzag(self):
        value = self.varint()
        return (value >> 1) ^ -(value & 1)

    def value(self, value_type):
        if value_type in (BOOL_TRUE, BOOL_FALSE):
            return value_type == BOOL_TRUE
        if value_type == BYTE:
            return self.byte()
        if value_type in (I16, I32, I64):
            return self.zigzag()
        if value_type == DOUBLE:
            return self.take(8)
        if value_type == BINARY:
            return self.take(self.varint())
        if value_type in (LIST, SET):
            header = self.byte()
            size = header >> 4
            element_type = header & 0x0f
            if size == 15:
                size = self.varint()
            return element_type, [self.value(element_type) for unused in range(size)]
        if value_type == MAP:
            size = self.varint()
            if size == 0:
                return STOP, STOP, []
            types = self.byte()
            key_type = types >> 4
            item_type = types & 0x0f
            items = [(self.value(key_type), self.value(item_type)) for unused in range(size)]
            return key_type, item_type, items
        if value_type == STRUCT:
            return self.struct()
        raise DecodeError("unknown compact-Thrift type {}".format(value_type))

    def struct(self):
        fields = []
        previous_id = 0
        while True:
            header = self.byte()
            if header == STOP:
                return fields
            value_type = header & 0x0f
            delta = header >> 4
            field_id = previous_id + delta if delta else self.zigzag()
            fields.append([field_id, value_type, self.value(value_type)])
            previous_id = field_id


def put_varint(output, value):
    while value >= 0x80:
        output.append((value & 0x7f) | 0x80)
        value >>= 7
    output.append(value)


def put_zigzag(output, value):
    put_varint(output, (value << 1) ^ (value >> 63))


def write_value(output, value_type, value):
    if value_type in (BOOL_TRUE, BOOL_FALSE):
        return
    if value_type == BYTE:
        output.append(value)
        return
    if value_type in (I16, I32, I64):
        put_zigzag(output, value)
        return
    if value_type == DOUBLE:
        output.extend(value)
        return
    if value_type == BINARY:
        put_varint(output, len(value))
        output.extend(value)
        return
    if value_type in (LIST, SET):
        element_type, items = value
        size = len(items)
        if size < 15:
            output.append((size << 4) | element_type)
        else:
            output.append(0xf0 | element_type)
            put_varint(output, size)
        for item in items:
            if element_type in (BOOL_TRUE, BOOL_FALSE):
                output.append(BOOL_TRUE if item else BOOL_FALSE)
            else:
                write_value(output, element_type, item)
        return
    if value_type == MAP:
        key_type, item_type, items = value
        put_varint(output, len(items))
        if items:
            output.append((key_type << 4) | item_type)
            for key, item in items:
                write_value(output, key_type, key)
                write_value(output, item_type, item)
        return
    if value_type == STRUCT:
        output.extend(write_struct(value))
        return
    raise ValueError("unknown compact-Thrift type {}".format(value_type))


def write_struct(fields):
    output = bytearray()
    previous_id = 0
    for field_id, value_type, value in fields:
        if value_type in (BOOL_TRUE, BOOL_FALSE):
            value_type = BOOL_TRUE if value else BOOL_FALSE
        delta = field_id - previous_id
        if 0 < delta <= 15:
            output.append((delta << 4) | value_type)
        else:
            output.append(value_type)
            put_zigzag(output, field_id)
        write_value(output, value_type, value)
        previous_id = field_id
    output.append(STOP)
    return bytes(output)


def decode(data):
    reader = Reader(data)
    result = reader.struct()
    if reader.offset != len(data):
        raise DecodeError("{} trailing bytes".format(len(data) - reader.offset))
    return result


def decode_prefix(data):
    """Decode one leading compact-Thrift struct and return it with bytes consumed."""
    reader = Reader(data)
    result = reader.struct()
    return result, reader.offset


def encode(fields):
    return write_struct(fields)
