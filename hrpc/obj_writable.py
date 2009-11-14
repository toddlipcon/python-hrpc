#!/usr/bin/env python2.4

from hrpc.writable import *

PRIMITIVE_TYPES = {}
ARRAY_CHARS = {}

class Type(object):
  def __init__(self, clazz, array_char, reader, writer):
    self.clazz = clazz
    self.array_char = array_char
    self.reader = reader
    self.writer = writer

    PRIMITIVE_TYPES[clazz] = self
    if array_char:
      ARRAY_CHARS[array_char] = self

  def write(self, out, val):
    self.writer(out, val)

  def read(self, ins):
    return self.reader(ins)

# Functions for dealing with java class name mangling
def _is_array(clazz):
  return clazz.startswith("[")

def _get_array_class(clazz):
  assert _is_array(clazz)
  if clazz[1] in ARRAY_CHARS:
    return ARRAY_CHARS[clazz[1]].clazz
  elif clazz[1] == 'L':
    # Array of some clazz
    assert clazz.endswith(";")
    return clazz[2:-1]
  else:
    raise IOError("Bad array class: " + clazz)

Bool = Type("boolean", "Z", read_bool, write_bool)
Byte = Type("byte", "B", read_byte, write_byte)
Char = Type("char", "C", read_char, write_char)
#Double = Type("double", "D", read_double, write_double)
#Float = Type("float", "F", read_float, write_float)
Int = Type("int", "I", read_int, write_int)
Long = Type("long", "J", read_long, write_long)
Short = Type("short", "S", read_short, write_short)
String = Type("java.lang.String", None, read_utf8, write_utf8)
Void = Type("void", None, read_utf8, write_utf8)

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


class Null(object):
  clazz = "org.apache.hadoop.io.ObjectWritable$NullInstance"

  def read(self, ins):
    self.declared_clazz = read_utf8(ins)


class Void(object):
  clazz = "org.apache.hadoop.io.ObjectWritable$NullInstance"

  def read(self, ins):
    declared_clazz = read_utf8(ins)
    assert declared_clazz == "void"

def write_object(out, obj_type, data):
  write_utf8(out, obj_type.clazz)
  if isinstance(obj_type, type) and issubclass(obj_type, (Writable,)):
    assert isinstance(data, obj_type)
    write_utf8(out, data.clazz)
    data.write(out)
  else:
    obj_type.write(out, data)

def read_object(ins, obj_type):
  declared_clazz = read_utf8(ins)
  primitive = PRIMITIVE_TYPES.get(declared_clazz)

  if primitive:
    # We expect that for primitives, we were expecting the type
    # we got.
    if declared_clazz != obj_type.clazz:
      raise IOError("Expected " + obj_type.clazz +
                    " and got " + declared_clazz)
    return obj_type.read(ins)

  elif _is_array(declared_clazz):
    assert isinstance(obj_type, Array)
    component_type = _get_array_class(declared_clazz)
    assert component_type == obj_type.elem_type.clazz
    length = read_int(ins)
    ret = []
    for i in xrange(0, length):
      ret.append(read_object(ins, obj_type.elem_type))
    return ret
  elif declared_clazz == "java.lang.String":
    assert obj_type == String
    return read_utf8(ins)
  else:
    real_clazz = read_utf8(ins)
    instance = obj_type()
    instance.read(ins)
    return instance

  assert False
