#!/usr/bin/env python2.4

import unittest
from hrpc import writable
from cStringIO import StringIO

class WritableTest(unittest.TestCase):
  def test_vints(self):
    def do_test(to_write):
      buf = StringIO()
      writable.write_vint(buf, to_write)
      buf.reset()
      print "encoded ", to_write, ": ", repr(buf.getvalue())
      readback = writable.read_vint(buf)
      self.assertEqual(to_write, readback)
    for i in [1, 100, -100, 130, -1000, 1000, 100000,
              -100000, 1000000000, -1000000000]:
      do_test(i)
