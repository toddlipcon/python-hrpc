#!/usr/bin/env python2.4

from hrpc import writable, obj_writable
from cStringIO import StringIO
import socket

STATUS_SUCCESS = 0
STATUS_EXCEPTION = 1

class Client(object):
  RPC_VERSION = 3

  def __init__(self, protocol):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.stream = StreamWrapper(self.sock)
    self.proto = protocol
    self.call_id = 1

  def connect(self, host, port):
    self.sock.connect((host, port))
    self.stream.write("hrpc")

    writable.write_byte(self.stream, self.RPC_VERSION)
    header = self._get_conn_header()
    self._send_frame(header)

  def _call(self, method_name, prototype, args):
    buf = StringIO()
    writable.write_int(buf, self.call_id)
    writable.write_utf8(buf, method_name)
    prototype.write_params(buf, args)
    self._send_frame(buf.getvalue())

    # Wait for response
    call_id_back = writable.read_int(self.stream)
    status_back = writable.read_int(self.stream)
    if call_id_back != self.call_id:
      raise IOError("Mismatched call ids- sent %d got %d" % (self.call_id, call_id_back))
    self.call_id += 1
    print "call id " + str(call_id_back)
    if status_back == STATUS_SUCCESS:
      print "success status"
      return prototype.read_response(self.stream)
    elif status_back == STATUS_EXCEPTION:
      print "error status"
      raise self._read_exception()
    else:
      raise IOError("Unknown status code: " + status_back)

  def _read_exception(self):
    clazz = writable.read_string(self.stream)
    message = writable.read_string(self.stream)
    return IOError("Java exception: %s\n\n%s" % (clazz, message))

  def _get_conn_header(self):
    buf = StringIO()
    writable.write_text(buf, self.proto.java_class)
    writable.write_bool(buf, True)
    UGI("todd", ["todd"]).write(buf)
    return buf.getvalue()

  def _send_frame(self, frame):
    writable.write_int(self.stream, len(frame))
    self.stream.write(frame)

  @property
  def proxy(self):
    return ClientProxy(self)

class ClientProxy(object):
  def __init__(self, client):
    self.__client = client

  def __getattr__(self, attr):
    prototype = getattr(self.__client.proto, attr)
    if not isinstance(prototype, MethodPrototype):
      raise AttributeError("No such function %s in %s" %
                           (attr, self.__client.proto))

    def wrapper(*args):
      return self.__client._call(attr, prototype, args)
    return wrapper

class MethodPrototype(object):
  def __init__(self, param_types, ret_type):
    self.param_types = param_types
    self.ret_type = ret_type

  def write_params(self, out, data):
    writable.write_int(out, len(data))
    for t, d in zip(self.param_types, data):
      obj_writable.write_object(out, t, d)

  def read_response(self, ins):
    return obj_writable.read_object(ins, self.ret_type)

class StreamWrapper(object):
  def __init__(self, sock):
    self.sock = sock

  def read(self, length):
    nread = 0
    data = ""
    while len(data) < length:
      this_data = self.sock.recv(length - len(data))
      if this_data == '':
        raise IOError("socket broken")
      data += this_data
    return data

  def write(self, data):
    sent = 0
    while sent < len(data):
      n = self.sock.send(data[sent:])
      if n == 0:
        raise IOError("socket broken")
      sent += n
    return sent


class UGI(object):
  def __init__(self, user, groups):
    self.user = user
    self.groups = groups

  def write(self, out):
    writable.write_text(out, "STRING_UGI")
    writable.write_text(out, self.user)
    writable.write_vint(out, len(self.groups))
    for g in self.groups:
      writable.write_text(out, g)
