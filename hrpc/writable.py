#!/usr/bin/env python2.4

from struct import pack, unpack

def read_long(ins):
  return unpack(">q", ins.read(8))[0]

def read_int(ins):
  return unpack(">l", ins.read(4))[0]

def read_short(ins):
  return unpack(">h", ins.read(2))[0]

def read_byte(ins):
  return unpack(">b", ins.read(1))[0]

def read_bool(ins):
  return read_byte(ins) != 0

def read_utf8(ins):
  length = read_short(ins)
  return unicode(ins.read(length), 'utf8')

def read_text(ins):
  length = read_vint(ins)
  return unicode(ins.read(length), 'utf8')

def read_string(ins):
  length = read_int(ins)
  return unicode(ins.read(length), 'utf8')

def read_vint(ins):
  first_byte = read_byte(ins)
  vlen = _decode_vint_size(first_byte)
  if vlen == 1:
    return first_byte
  num_bytes = vlen - 1
  packed_bytes = "\0" * (8 - num_bytes) + ins.read(num_bytes)
  int_read = unpack(">q", packed_bytes)[0]
  if first_byte < -120 or (first_byte >= -112 and first_byte < 0):
    int_read = ~int_read
  return int_read

def _decode_vint_size(b):
  if b >= -112:
    return 1
  elif b < -120:
    return -119 - b
  else:
    return -111 - b


def write_long(out, n):
  out.write(pack(">q", n))

def write_int(out, n):
  out.write(pack(">l", n))

def write_short(out, n):
  out.write(pack(">h", n))

def write_byte(out, n):
  out.write(pack(">b", n))

def write_bool(out, b):
  out.write(b and "\x01" or "\x00")

def write_vint(out, n):
  if n >= -112 and n <= 127:
    write_byte(out, n)
    return

  if n < 0:
    length = -120
    n = ~n
  else:
    length = -112
  packed_long = pack(">q", n)
  for num_leading_zeros, c in enumerate(packed_long):
    if c != '\0':
      break
  adj_len = length - (8 - num_leading_zeros)
  out.write(pack(">b", adj_len))
  out.write(packed_long[num_leading_zeros:])

def write_text(out, text):
  encoded = text.encode('utf8')
  write_vint(out, len(encoded))
  out.write(encoded)

def write_utf8(out, text):
  encoded = text.encode('utf8')
  write_short(out, len(encoded))
  out.write(encoded)

def write_string(out, str):
  encoded = text.encode('utf8')
  write_int(out, len(encoded))
  out.write(encoded)

def write_object(out, obj_type, data):
  write_utf8(out, obj_type.type_identifier)
  obj_type.write(out, data)

def read_object(ins, obj_type):
  clazz = read_utf8(ins)
  if clazz != obj_type.type_identifier:
    raise IOError("Expected " + obj_type.type_identifier +
                  " and got " + clazz)

  if isinstance(obj_type, type) and issubclass(obj_type, (Writable,)):
    # it sends the declared class first, then the real class, I think
    # but it should always be the same as far as I know!
    real_clazz = read_utf8(ins)
    assert real_clazz == clazz
    instance = obj_type()
    instance.read(ins)
    return instance
  else:
    return obj_type.read(ins)

class Obj(object):
  class Long(object):
    type_identifier = "long"
    short_identifier = "J"
    @staticmethod
    def write(out, n):
      write_long(out, n)

    @staticmethod
    def read(ins):
      return read_long(ins)

  class String(object):
    type_identifier = "java.lang.String"
    @staticmethod
    def write(out, s):
      write_utf8(out, s)
    @staticmethod
    def read(ins):
      return read_utf8(ins)

  class Array(object):
    def __init__(self, elem_type):
      self.elem_type = elem_type

    @property
    def type_identifier(self):
      if hasattr(self.elem_type, 'short_identifier'):
        short_name = self.elem_type.short_identifier
      else:
        short_name = self.elem_type.type_identifier
      return "[" + short_name

    def read(self, ins):
      arraylen = read_int(ins)
      ret = []
      for i in xrange(0, arraylen):
        ret.append(read_object(ins, self.elem_type))
      return ret


class Writable(object):
  pass

class LongWritable(Writable):
  def __init__(self, n=0):
    self.n = n
  def write(self, out):
    write_long(out, self.n)
  def read(self, ins):
    self.n = read_long(ins)

class IntWritable(Writable):
  def __init__(self, n=0):
    self.n = n
  def write(self, out):
    write_int(out, self.n)
  def read(self, ins):
    self.n = read_int(ins)


class ByteWritable(Writable):
  def __init__(self, n):
    self.n = n
  def write(self, out):
    write_byte(out, self.n)
  def read(self, ins):
    self.n = read_byte(ins)

class VIntWritable(Writable):
  def __init__(self, n):
    self.n = n
  def write(self, out):
    write_vint(out, self.n)
  def read(self, ins):
    self.n = read_vint(ins)

class Utf8(Writable):
  def __init__(self, s):
    self.s = s

  def write(out):
    encoded = self.s.encode("utf8")
    out.write(pack(">h", len(encoded)))
    out.write(encoded)

