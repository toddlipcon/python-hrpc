#!/usr/bin/python2.4

from hrpc import client, writable
from hrpc.writable import Obj, Writable
from hrpc.client import MethodPrototype

class LocatedBlocks(Writable):
  type_identifier = "org.apache.hadoop.hdfs.protocol.LocatedBlocks"
  def __init__(self):
    self.file_length = 0
    self.under_construction = False
    self.blocks = []


  def write(self, out):
    writable.write_long(out, self.file_length)
    writable.write_bool(out, self.under_construction)
    writable.write_int(len(out, self.blocks))
    for b in self.blocks:
      b.write(out)

  def read(self, ins):
    __import__("pdb").set_trace()
    self.file_length = writable.read_long(ins)
    self.under_construction = writable.read_bool(ins)
    num_blocks = writable.read_int(ins)
    self.blocks = []
    for i in xrange(0, num_blocks):
      b = LocatedBlock()
      b.read(ins)
      self.blocks.append(b)

class LocatedBlock(Writable):
  def __init__(self):
    self.offset = 0
    self.corrupt = False
    self.block = Block()
    self.locs = []

  def write(self, out):
    writable.write_bool(out, self.corrupt)
    writable.write_long(out, self.offset)
    self.block.write(out)
    writable.write_int(out, len(self.locs))
    for l in self.locs:
      l.write(out)

  def read(self, ins):
    self.corrupt = writable.read_bool(ins)
    self.offset = writable.read_long(ins)
    self.block = Block()
    self.block.read(ins)
    num_locs = writable.read_int(ins)
    self.locs = []
    for i in xrange(0, num_locs):
      l = DatanodeInfo()
      l.read(ins)
      self.locs.append(l)

class Block(Writable):
  def __init__(self):
    self.block_id = 0
    self.num_bytes = 0
    self.gen_stamp = 0

  def write(self, out):
    writable.write_long(out, self.block_id)
    writable.write_long(out, self.num_bytes)
    writable.write_long(out, self.gen_stamp)

  def read(self, ins):
    self.block_id = writable.read_long(ins)
    self.num_bytes = writable.read_long(ins)
    self.gen_stamp = writable.read_long(ins)

class DatanodeID(Writable):
  def __init__(self):
    pass

  def read(self, ins):
    self.name = writable.read_utf8(ins)
    self.storage_id = writable.read_utf8(ins)
    self.info_port = writable.read_short(ins) & 0xffff

  # TODO write

class DatanodeInfo(DatanodeID):
  def __init__(self):
    self.ipc_port = 0
    self.capacity = 0
    self.dfs_used = 0
    self.remaining = 0
    self.last_update = 0
    self.xceiver_count = 0
    self.location = ""
    self.hostname = None
    self.admin_state = "NORMAL"

  def write(self, out):
    DatanodeID.write(self, out)
    writable.write_short(out, self.ipc_port)
    for l in [self.capacity, self.dfs_used, self.remaining, self.last_update]:
      writable.write_long(out, l)
    writable.write_int(out, self.xceiver_count)
    writable.write_text(out, self.location)
    writable.write_text(out, self.hostname or "")

    writable.write_text(out, self.admin_state) # really enum

  def read(self, ins):
    DatanodeID.read(self, ins)
    self.ipc_port = writable.read_short(ins) & 0xffff
    self.capacity = writable.read_long(ins)
    self.dfs_used = writable.read_long(ins)
    self.remaining = writable.read_long(ins)
    self.last_update = writable.read_long(ins)

    self.xceiver_count = writable.read_int(ins)
    __import__("pdb").set_trace()
    self.location = writable.read_text(ins)
    self.hostname = writable.read_text(ins)
    self.admin_state = writable.read_text(ins)

class VersionedProtocol(object):
  java_class = "org.apache.hadoop.ipc.VersionedProtocol"

  getProtocolVersion = MethodPrototype(
    [writable.Obj.String, writable.Obj.Long],
    writable.Obj.Long)

class ClientProtocol(VersionedProtocol):
  java_class = "org.apache.hadoop.hdfs.protocol.ClientProtocol"

  getStats = MethodPrototype(
    [],
    writable.Obj.Array(writable.Obj.Long))

  getBlockLocations = MethodPrototype(
    [writable.Obj.String, writable.Obj.Long, writable.Obj.Long],
    LocatedBlocks)



c = client.Client(ClientProtocol)
c.connect("127.0.0.1", 8020)
print c.proxy.getProtocolVersion(ClientProtocol.java_class, 0)
print c.proxy.getStats()
print c.proxy.getBlockLocations("/user/todd/grepout/part-00000", 0, 1000)

