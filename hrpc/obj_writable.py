#!/usr/bin/env python2.4

from hrpc.writable import *

TYPES = {}

class Type(object):
  def __init__(self, clazz, array_char, reader, writer):
    self.clazz = clazz
    self.array_char = array_char
    self.reader = reader
    self.writer = writer

    TYPES[clazz] = self


  def write(self, out, val):
    self.writer(out, val)

  def read(self, ins):
    return self.reader(ins)


Bool = Type("boolean", "Z", read_bool, write_bool)
Byte = Type("byte", "B", read_byte, write_byte)
Char = Type("char", "C", read_char, write_char)
#Double = Type("double", "D", read_double, write_double)
#Float = Type("float", "F", read_float, write_float)
Int = Type("int", "I", read_int, write_int)
Long = Type("long", "J", read_long, write_long)
Short = Type("short", "S", read_short, write_short)
String = Type("java.lang.String", None, read_utf8, write_utf8)


class Enum(object):
  def __init__(self, clazz):
    self.clazz = clazz

  @staticmethod
  def write(out, val):
    write_utf8(out, val)

def _array_class(elem_type):
  if hasattr(elem_type, "array_char"):
    return "[" + elem_type.array_char
  else:
    return "[L" + elem_type.clazz + ";"


class Array(object):
  def __init__(self, elem_type):
    self.elem_type = elem_type

  @property
  def clazz(self):
    return _array_class(self.elem_type)

  def read(self, ins):
    arraylen = read_int(ins)
    ret = []
    for i in xrange(0, arraylen):
      ret.append(read_object(ins, self.elem_type))
    return ret

def write_object(out, obj_type, data):
  write_utf8(out, obj_type.clazz)
  if isinstance(obj_type, type) and issubclass(obj_type, (Writable,)):
    assert isinstance(data, obj_type)
    write_utf8(out, data.type_identifier)
    data.write(out)
  else:
    obj_type.write(out, data)

def read_object(ins, obj_type):
  clazz = read_utf8(ins)
  if clazz != obj_type.clazz:
    raise IOError("Expected " + obj_type.clazz +
                  " and got " + clazz)

  if isinstance(obj_type, type) and issubclass(obj_type, (Writable,)):
    # it sends the declared class first, then the real class, I think
    # but it should always be the same as far as I know!
    real_clazz = read_utf8(ins)
    assert real_clazz == "org.apache.hadoop.io.Writable" or real_clazz == clazz
    instance = obj_type()
    instance.read(ins)
    return instance
  else:
    return obj_type.read(ins)
