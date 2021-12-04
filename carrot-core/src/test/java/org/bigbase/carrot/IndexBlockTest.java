/*
 Copyright (C) 2021-present Carrot, Inc.

 <p>This program is free software: you can redistribute it and/or modify it under the terms of the
 Server Side Public License, version 1, as published by MongoDB, Inc.

 <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 Server Side Public License for more details.

 <p>You should have received a copy of the Server Side Public License along with this program. If
 not, see <http://www.mongodb.com/licensing/server-side-public-license>.
*/
package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

public class IndexBlockTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(IndexBlockTest.class);

  public IndexBlockTest(Object c, Object m) {
    super(c, m);
  }

  @Override
  public void extTearDown() {}

  protected void freeKeys(ArrayList<Key> keys) {
    for (Key key : keys) {
      UnsafeAccess.free(key.address);
    }
  }

  @Test
  public void testPutGet() {
    log.debug(testName.getMethodName());

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    for (Key key : keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertEquals(size, key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    ib.free();
    freeKeys(keys);
  }

  @Test
  public void testAutomaticDataBlockMerge() {
    log.debug(testName.getMethodName());

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);

    int before = ib.getNumberOfDataBlock();

    // Delete half of records

    List<Key> toDelete = keys.subList(0, keys.size() / 2);
    for (Key key : toDelete) {
      OpResult res = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertSame(res, OpResult.OK);
    }
    int after = ib.getNumberOfDataBlock();
    log.debug("Before ={} After={}", before, after);
    assertTrue(before > after);
    ib.free();
    freeKeys(keys);
  }

  @Test
  public void testPutGetDeleteFull() {
    log.debug(testName.getMethodName());

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for (Key key : keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);

      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertEquals(size, key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }

    // now delete all
    List<Key> splitRequires = new ArrayList<>();
    for (Key key : keys) {
      OpResult result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      if (result == OpResult.SPLIT_REQUIRED) {
        splitRequires.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for (Key key : keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);

      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      if (splitRequires.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertEquals(DataBlock.NOT_FOUND, size);
      }
      UnsafeAccess.free(valuePtr);
    }
    ib.free();
    freeKeys(keys);
  }

  @Test
  public void testPutGetDeletePartial() {
    log.debug(testName.getMethodName());

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for (Key key : keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertEquals(size, key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }

    // now delete some
    List<Key> toDelete = keys.subList(0, keys.size() / 2);
    List<Key> splitRequires = new ArrayList<>();

    for (Key key : toDelete) {

      OpResult result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      if (result == OpResult.SPLIT_REQUIRED) {
        splitRequires.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for (Key key : toDelete) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      if (splitRequires.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertEquals(DataBlock.NOT_FOUND, size);
      }
      UnsafeAccess.free(valuePtr);
    }
    // Now get the rest
    for (Key key : keys.subList(keys.size() / 2, keys.size())) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertEquals(size, key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }

    ib.free();
    freeKeys(keys);
  }

  @Test
  public void testOverwriteSameValueSize() throws RetryOperationException, IOException {
    log.debug(testName.getMethodName());

    Random r = new Random();
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    for (Key key : keys) {
      byte[] value = new byte[key.length];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = ib.put(key.address, key.length, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = ib.get(key.address, key.length, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      assertEquals(0, Utils.compareTo(buf, value.length, valuePtr, value.length));
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(ib, keys);
    ib.free();
    freeKeys(keys);
  }

  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    log.debug(testName.getMethodName());

    Random r = new Random();
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    for (Key key : keys) {
      byte[] value = new byte[key.length - 2];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = ib.put(key.address, key.length, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = ib.get(key.address, key.length, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      assertEquals(0, Utils.compareTo(buf, value.length, valuePtr, value.length));
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(ib, keys);
    ib.free();
    freeKeys(keys);
  }

  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    log.debug(testName.getMethodName());

    Random r = new Random();
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    // Delete half keys
    int toDelete = keys.size() / 2;
    for (int i = 0; i < toDelete; i++) {
      Key key = keys.remove(0);
      OpResult res = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
      UnsafeAccess.free(key.address);
    }
    for (Key key : keys) {
      byte[] value = new byte[key.length + 2];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = ib.put(key.address, key.length, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = ib.get(key.address, key.length, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      assertEquals(0, Utils.compareTo(buf, value.length, valuePtr, value.length));
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(ib, keys);
    ib.free();
    freeKeys(keys);
  }

  protected void scanAndVerify(IndexBlock b, List<Key> keys)
      throws RetryOperationException, IOException {
    long buffer;
    long tmp = 0L;
    int prevLength = 0;
    IndexBlockScanner is = IndexBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
    DataBlockScanner bs;
    int count = 0;
    while ((bs = is.nextBlockScanner()) != null) {
      while (bs.hasNext()) {
        int len = bs.keySize();
        buffer = UnsafeAccess.malloc(len);
        bs.key(buffer, len);

        boolean result = contains(buffer, len, keys);
        assertTrue(result);
        bs.next();
        count++;
        if (count > 1) {
          // compare
          int res = Utils.compareTo(tmp, prevLength, buffer, len);
          assertTrue(res < 0);
          UnsafeAccess.free(tmp);
        }
        tmp = buffer;
        prevLength = len;
      }
      bs.close();
    }
    UnsafeAccess.free(tmp);
    is.close();
    assertEquals(keys.size(), count);
  }

  private boolean contains(long key, int size, List<Key> keys) {
    for (Key k : keys) {
      if (Utils.compareTo(k.address, k.length, key, size) == 0) {
        return true;
      }
    }
    return false;
  }

  protected IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(null, size);
    ib.setFirstIndexBlock();
    return ib;
  }

  protected ArrayList<Key> fillIndexBlock(IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Fill seed={}", seed);
    int kvSize = 32;
    boolean result = true;
    while (result) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(kvSize);
      UnsafeAccess.copy(key, 0, ptr, kvSize);
      result = b.put(ptr, kvSize, ptr, kvSize, 0, 0);
      if (result) {
        keys.add(new Key(ptr, kvSize));
      } else {
        UnsafeAccess.free(ptr);
      }
    }
    log.debug(
        "Number of data blocks={} index block data size ={} num records={}",
        b.getNumberOfDataBlock(),
        b.getDataInBlockSize(),
        keys.size());
    return keys;
  }
}
