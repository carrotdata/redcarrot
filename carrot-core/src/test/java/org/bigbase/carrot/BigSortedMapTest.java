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
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

// TODO: MEMORY LEAK
public class BigSortedMapTest {

  private static final Logger log = LogManager.getLogger(BigSortedMapTest.class);

  BigSortedMap map;
  long totalLoaded;
  long MAX_ROWS = 1000000;

  static {
    // UnsafeAccess.debug = true;
  }

  long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long counter = 0;
    while (scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }

  private boolean load(long totalLoaded) {
    byte[] key = ("KEY" + (totalLoaded)).getBytes();
    byte[] value = ("VALUE" + (totalLoaded)).getBytes();
    long keyPtr = UnsafeAccess.malloc(key.length);
    UnsafeAccess.copy(key, 0, keyPtr, key.length);
    long valPtr = UnsafeAccess.malloc(value.length);
    UnsafeAccess.copy(value, 0, valPtr, value.length);
    boolean result = map.put(keyPtr, key.length, valPtr, value.length, 0);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valPtr);
    return result;
  }

  public void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000);
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

  public void tearDown() {
    map.dispose();
  }

  private void allTests() throws IOException {
    testDeleteUndeleted();
    testExists();
    testPutGet();

    // This call must be the last
    testFlushAll();
  }

  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 1; i++) {
      log.debug("\n********* {} ********** Codec = NONE\n", i);
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }

  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    for (int i = 0; i < 1; i++) {
      log.debug("\n********* {} ********** Codec = LZ4\n", i);
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }

  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    for (int i = 0; i < 1; i++) {
      log.debug("\n********* {} ********** Codec = LZ4HC\n", i);
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }

  @Ignore
  @Test
  public void testDeleteUndeleted() throws IOException {
    log.debug("testDeleteUndeleted");
    List<byte[]> keys = delete(100);
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());
  }

  @Ignore
  @Test
  public void testPutGet() {
    log.debug("testPutGet");

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
        assertTrue(Utils.compareTo(value, 0, value.length, valPtr, (int) size) == 0);
      } catch (Throwable t) {
        throw t;
      } finally {
        UnsafeAccess.free(keyPtr);
        UnsafeAccess.free(valPtr);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to get {} ={}ms", totalLoaded, (end - start));
  }

  @Ignore
  @Test
  public void testExists() {
    log.debug("testExists");

    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + (i)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean res = map.exists(keyPtr, key.length);
      UnsafeAccess.free(keyPtr);
      assertEquals(true, res);
    }
  }

  @Ignore
  @Test
  public void testFlushAll() {
    log.debug("testFlushAll");

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
    List<byte[]> list = new ArrayList<byte[]>();
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
        continue;
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
