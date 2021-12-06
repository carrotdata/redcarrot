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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

// TODO: MEMORY LEAK
public class BigSortedMapTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(BigSortedMapTest.class);

  long totalLoaded;
  long MAX_ROWS = 1000000;

  public BigSortedMapTest(Object c) {
    super(c);
    BigSortedMap.setMaxBlockSize(4096);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();

    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while (totalLoaded < MAX_ROWS) {
      totalLoaded++;
      load(totalLoaded);
      if (totalLoaded % 100000 == 0) {
        log.debug("Loaded {}", totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to load= {} ={}ms", totalLoaded, (end - start));
    long scanned = countRecords();
    log.debug("Scanned={}", countRecords());
    log.debug("\nTotal memory     ={}", BigSortedMap.getGlobalAllocatedMemory());
    log.debug("Total   data       ={}", BigSortedMap.getGlobalDataSize());
    log.debug("Compressed size    ={}", BigSortedMap.getGlobalCompressedDataSize());
    log.debug(
            "Compression  ratio ={}",
            ((float) BigSortedMap.getGlobalDataSize()) / BigSortedMap.getGlobalAllocatedMemory());
    log.debug("");
    assertEquals(totalLoaded, scanned);
  }

  @Override
  public void extTearDown() {
  }

  private long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long counter = 0;
    while (scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }

  private void load(long totalLoaded) {
    byte[] key = ("KEY" + (totalLoaded)).getBytes();
    byte[] value = ("VALUE" + (totalLoaded)).getBytes();
    long keyPtr = UnsafeAccess.malloc(key.length);
    UnsafeAccess.copy(key, 0, keyPtr, key.length);
    long valPtr = UnsafeAccess.malloc(value.length);
    UnsafeAccess.copy(value, 0, valPtr, value.length);
    boolean result = map.put(keyPtr, key.length, valPtr, value.length, 0);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valPtr);
    log.debug("loaded: {}", result);
  }

  @Test
  public void testCumulative() throws IOException {
    deleteUndeleted();
    exists();
    putGet();

    // This call must be the last
    flushAll();
  }

  private void deleteUndeleted() throws IOException {
    log.debug("{}", testName.getMethodName());

    List<byte[]> keys = delete(100);
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());
  }

  private void putGet() {
    log.debug("{}", testName.getMethodName());

    long start = System.currentTimeMillis();
    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + (i)).getBytes();
      byte[] value = ("VALUE" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);

      try {
        long size = map.get(keyPtr, key.length, valPtr, value.length, Long.MAX_VALUE);
        assertEquals(value.length, (int) size);
        assertEquals(0, Utils.compareTo(value, 0, value.length, valPtr, (int) size));
      } finally {
        UnsafeAccess.free(keyPtr);
        UnsafeAccess.free(valPtr);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to get {} ={}ms", totalLoaded, (end - start));
  }

  private void exists() {
    log.debug("{}", testName.getMethodName());

    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + (i)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean res = map.exists(keyPtr, key.length);
      UnsafeAccess.free(keyPtr);
      assertTrue(res);
    }
  }

  private void flushAll() {
    log.debug("{}", testName.getMethodName());

    map.flushAll();

    // Check that all keys have been deleted
    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + (i)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean res = map.exists(keyPtr, key.length);
      UnsafeAccess.free(keyPtr);
      assertFalse(res);
    }
  }

  private List<byte[]> delete(int num) {
    Random r = new Random();
    int numDeleted = 0;
    long valPtr = UnsafeAccess.malloc(1);
    List<byte[]> list = new ArrayList<>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int) totalLoaded) + 1;
      byte[] key = ("KEY" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long len = map.get(keyPtr, key.length, valPtr, 1, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
        UnsafeAccess.free(keyPtr);
      } else {
        boolean res = map.delete(keyPtr, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
        UnsafeAccess.free(keyPtr);
      }
    }
    UnsafeAccess.free(valPtr);
    log.debug("Deleted={} collisions={}", numDeleted, collisions);
    return list;
  }

  private void undelete(List<byte[]> keys) {
    for (byte[] key : keys) {
      byte[] value = ("VALUE" + new String(key).substring(3)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, valPtr, value.length);
      boolean res = map.put(keyPtr, key.length, valPtr, value.length, 0);
      UnsafeAccess.free(valPtr);
      UnsafeAccess.free(keyPtr);
      assertTrue(res);
    }
  }
}
