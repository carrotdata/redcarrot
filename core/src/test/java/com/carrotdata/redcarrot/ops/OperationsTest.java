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
package com.carrotdata.redcarrot.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.BigSortedMapScanner;
import com.carrotdata.redcarrot.ops.Append;
import com.carrotdata.redcarrot.ops.IncrementLong;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import org.junit.Test;

public class OperationsTest {

  private static final Logger log = LogManager.getLogger(OperationsTest.class);

  static BigSortedMap map;
  static long totalLoaded;

  public void load() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    byte[] LONG_ZERO = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
    while (true) {
      totalLoaded++;
      byte[] key = ("KEY" + (totalLoaded)).getBytes();
      byte[] value = LONG_ZERO;
      long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      int keySize = key.length;
      int valueSize = value.length;
      boolean result = map.put(keyPtr, keySize, valuePtr, valueSize, -1);
      UnsafeAccess.free(keyPtr);
      UnsafeAccess.free(valuePtr);

      if (result == false) {
        totalLoaded--;
        break;
      }
      if (totalLoaded % 1000000 == 0) {
        log.debug("Loaded = {}", totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    map.dumpStats();
    log.debug("Time to load= {} = {}ms", totalLoaded, end - start);
    log.debug("Total memory={}", BigSortedMap.getGlobalAllocatedMemory());
    log.debug("Total   data={}", BigSortedMap.getGlobalBlockDataSize());
    log.debug("Total  index={}", BigSortedMap.getGlobalBlockIndexSize());
  }

  @Test
  public void testIncrement() throws IOException {
    try {
      log.debug("Increment test");
      load();
      IncrementLong incr = new IncrementLong();
      long ptr = UnsafeAccess.malloc(16);
      int keySize;
      long totalIncrement = 2000000;
      Random r = new Random();
      long start = System.currentTimeMillis();
      for (int i = 0; i < totalIncrement; i++) {
        int n = r.nextInt((int) totalLoaded) + 1;
        keySize = getKey(ptr, n);
        incr.reset();
        incr.setIncrement(1);
        incr.setKeyAddress(ptr);
        incr.setKeySize(keySize);
        boolean res = map.execute(incr);
        assertTrue(res);
      }
      long end = System.currentTimeMillis();
      log.debug("Time to increment {} {}ms", totalIncrement, end - start);
      BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
      long total = 0;
      while (scanner.hasNext()) {
        long addr = scanner.valueAddress();
        total += UnsafeAccess.toLong(addr);
        scanner.next();
      }

      assertEquals(totalIncrement, total);
    } finally {
      if (map != null) {
        map.dispose();
        map = null;
      }
    }
  }

  @Test
  public void testAppend() throws IOException {
    try {
      log.debug("Append test");
      loadForAppend();
      Append append = new Append();
      long key = UnsafeAccess.malloc(16);
      long value = UnsafeAccess.malloc(8);
      int keySize;
      long totalAppend = 2000000;
      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      log.debug("SEED={}", seed);
      long start = System.currentTimeMillis();
      for (int i = 0; i < totalAppend; i++) {

        int n = r.nextInt((int) totalLoaded) + 1;
        keySize = getKey(key, n);
        // log.debug(" i={}", i);
        append.reset();
        append.setKeyAddress(key);
        append.setKeySize(keySize);
        append.setAppendValue(value, 8);
        boolean res = map.execute(append);
        assertTrue(res);
      }
      long end = System.currentTimeMillis();
      log.debug("Time to append {} {}ms", totalAppend, end - start);
      BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
      long total = 0;
      while (scanner.hasNext()) {
        int size = scanner.valueSize();
        total += size;
        scanner.next();
      }

      assertEquals((totalAppend + totalLoaded) * 8, total);
    } finally {
      if (map != null) {
        map.dispose();
        map = null;
      }
    }
  }

  public void loadForAppend() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(1000000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    byte[] LONG_ZERO = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
    int count = 0;
    while (count++ < 2000000) {
      totalLoaded++;
      byte[] key = ("KEY" + (totalLoaded)).getBytes();
      byte[] value = LONG_ZERO;
      long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      int keySize = key.length;
      int valueSize = value.length;
      boolean result = map.put(keyPtr, keySize, valuePtr, valueSize, -1);
      UnsafeAccess.free(keyPtr);
      UnsafeAccess.free(valuePtr);
      if (result == false) {
        totalLoaded--;
        break;
      }
      if (totalLoaded % 1000000 == 0) {
        log.debug("Loaded = {}", totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    map.dumpStats();
    log.debug("Time to load= {} ={}ms", totalLoaded, end - start);
    log.debug("Total memory={}", BigSortedMap.getGlobalAllocatedMemory());
    log.debug("Total   data={}", BigSortedMap.getGlobalBlockDataSize());
    log.debug("Total  index={}", BigSortedMap.getGlobalBlockIndexSize());
  }

  private int getKey(long ptr, int n) {
    // System.out.print(n);
    byte[] key = ("KEY" + (n)).getBytes();
    UnsafeAccess.copy(key, 0, ptr, key.length);
    return key.length;
  }
}
