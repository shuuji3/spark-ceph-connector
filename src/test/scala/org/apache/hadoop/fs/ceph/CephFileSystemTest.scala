package org.apache.hadoop.fs.ceph

import org.scalatest.FunSuite

class CephFileSystemTest extends FunSuite {

  test("testGetScheme") {
    // FIXME: The symbol CephFileSystem cannot be found error
    assert(CephFileSystem.getScheme() === "ceph")
  }
}
