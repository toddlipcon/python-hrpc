#!/usr/bin/python2.4

from hrpc import client, writable, obj_writable
from hrpc.writable import Writable
from hrpc.client import MethodPrototype

class LocatedBlocks(Writable):
  clazz = "org.apache.hadoop.hdfs.protocol.LocatedBlocks"
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
  clazz = "org.apache.hadoop.hdfs.protocol.DatanodeInfo"
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
    self.location = writable.read_text(ins)
    self.hostname = writable.read_text(ins)
    self.admin_state = writable.read_text(ins)

class FsPermission(Writable):
  clazz = "org.apache.hadoop.fs.permission.FsPermission"

  def __init__(self, mode=0):
    self.mode = mode

  def read(self, ins):
    self.mode = writable.read_short(ins)

  def write(self, out):
    writable.write_short(out, self.mode)

class FileStatus(Writable):
  clazz = "org.apache.hadoop.fs.FileStatus"

  # TODO init, write
  def read(self, ins):
    self.path = writable.read_text(ins)
    self.length = writable.read_long(ins)
    self.isdir = writable.read_bool(ins)
    self.block_replication = writable.read_short(ins)
    self.blocksize = writable.read_long(ins)
    self.modification_time = writable.read_long(ins)
    self.access_time  = writable.read_long(ins)

    self.permission = FsPermission()
    self.permission.read(ins)

    self.owner = writable.read_text(ins)
    self.group = writable.read_text(ins)


class VersionedProtocol(object):
  java_class = "org.apache.hadoop.ipc.VersionedProtocol"

  getProtocolVersion = MethodPrototype(
    [obj_writable.String, obj_writable.Long],
    obj_writable.Long)

class ClientProtocol(VersionedProtocol):
  java_class = "org.apache.hadoop.hdfs.protocol.ClientProtocol"

  getStats = MethodPrototype(
    [],
    obj_writable.Array(obj_writable.Long))

  getBlockLocations = MethodPrototype(
    [obj_writable.String, obj_writable.Long, obj_writable.Long],
    LocatedBlocks)

  getDatanodeReport = MethodPrototype(
    [obj_writable.Enum("org.apache.hadoop.hdfs.protocol.FSConstants$DatanodeReportType")],
    obj_writable.Array(DatanodeInfo))

  getFileInfo = MethodPrototype(
    [obj_writable.String],
    FileStatus)

  getListing = MethodPrototype(
    [obj_writable.String],
    obj_writable.Array(FileStatus))

  setPermission = MethodPrototype(
    [obj_writable.String, FsPermission],
    obj_writable.Void)

c = client.Client(ClientProtocol)
c.connect("127.0.0.1", 8020)
print c.proxy.getProtocolVersion(ClientProtocol.java_class, 0)
print c.proxy.getStats()
print c.proxy.getBlockLocations("/user/todd/grepout/part-00000", 0, 1000)
print c.proxy.getDatanodeReport("ALL")
print c.proxy.getFileInfo("/user/todd/grepout/part-00000")
print c.proxy.getListing("/user/todd/grepout/")
c.proxy.setPermission("/user/todd/grepout/", FsPermission(493))
