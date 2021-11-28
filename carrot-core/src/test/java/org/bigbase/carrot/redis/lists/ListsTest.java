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
package org.bigbase.carrot.redis.lists;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.CarrotCoreBase;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.redis.lists.Lists.Side;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.bigbase.carrot.util.Value;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

public class ListsTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(ListsTest.class);

  static BigSortedMap map;
  static Key key;
  static List<Value> values;
  static long buffer;
  static int bufferSize = 64;
  static int keySize = 16;
  static int valueSize = 8;
  static int n = 100000;

  public ListsTest(Object c) throws IOException {
    super(c);
    tearDown();
    setUp();
  }

  private static Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("KEY SEED={}", seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return key = new Key(ptr, keySize);
  }

  private static Key getAnotherKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("ANOTHER KEY SEED={}", seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }

  private static List<Value> getValues(int n) {
    byte[] buf = new byte[valueSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("VALUES seed={}", seed);
    values = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.allocAndCopy(buf, 0, valueSize);
      values.add(new Value(ptr, valueSize));
    }
    return values;
  }

  public static void setUp() {
    map = new BigSortedMap(100000000L);
    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    values = getValues(n);
  }

  @AfterClass
  public static void tearDown() {
    if (Objects.isNull(map)) return;

    // Dispose
    map.dispose();

    DataBlock.clearDeallocators();
    DataBlock.clearSerDes();

    UnsafeAccess.free(key.address);
    UnsafeAccess.free(buffer);
    values.forEach(x -> UnsafeAccess.free(x.address));
    BigSortedMap.printGlobalMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();
  }


  @Test
  public void testDeallocator() {
    log.debug("Test Deallocator {}", getParameters());

    // Register LIST deallocator
    Lists.registerDeallocator();

    Key key = getKey();

    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testListWithLargeElements() {
    log.debug("Test List with large elements {}", getParameters());

    Key key = getKey();
    int largeSize = 1023;
    long largePtr = UnsafeAccess.malloc(largeSize);
    long bufPtr = UnsafeAccess.malloc(largeSize);

    byte[] buf = new byte[largeSize];
    Random r = new Random();
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, largePtr, largeSize);

    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(2L * i + 1, len);
      elemPtrs[0] = largePtr;
      elemSizes[0] = largeSize;
      len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(2L * i + 2, len);
    }

    for (int i = 0; i < n; i++) {
      int size = Lists.RPOP(map, key.address, key.length, bufPtr, largeSize);
      assertEquals(valueSize, size);
      assertEquals(0, Utils.compareTo(bufPtr, size, values.get(i).address, values.get(i).length));
      size = Lists.RPOP(map, key.address, key.length, bufPtr, largeSize);
      assertEquals(largeSize, size);
      assertEquals(0, Utils.compareTo(bufPtr, size, largePtr, largeSize));
    }

    UnsafeAccess.free(largePtr);
    UnsafeAccess.free(bufPtr);
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testListSerDeWithLargeElements() {
    log.debug("Test List SerDe with large elements {}", getParameters());

    // Register LIST deallocator
    Lists.registerDeallocator();
    Lists.registerSerDe();
    Key key = getKey();
    int largeSize = 1023;
    long largePtr = UnsafeAccess.malloc(largeSize);
    long bufPtr = UnsafeAccess.malloc(largeSize);

    byte[] buf = new byte[largeSize];
    Random r = new Random();
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, largePtr, largeSize);
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(2L * i + 1, len);
      elemPtrs[0] = largePtr;
      elemSizes[0] = largeSize;
      len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(2L * i + 2, len);
    }

    log.debug("Before BSM.dispose:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();

    log.debug("Taking snapshot");
    map.snapshot();
    map.dispose();

    log.debug("After BSM.dispose:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();

    // Loading store from snapshot
    BigSortedMap.setStatsUpdatesDisabled(true);
    map = BigSortedMap.loadStore(0);
    BigSortedMap.setStatsUpdatesDisabled(false);
    map.syncStatsToGlobal();
    // Data is ready
    log.debug("Load snapshot");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();
    // Verify data after load snapshot
    for (int i = 0; i < n; i++) {
      int size = Lists.RPOP(map, key.address, key.length, bufPtr, largeSize);
      assertEquals(valueSize, size);
      assertEquals(0, Utils.compareTo(bufPtr, size, values.get(i).address, values.get(i).length));
      size = Lists.RPOP(map, key.address, key.length, bufPtr, largeSize);
      assertEquals(largeSize, size);
      assertEquals(0, Utils.compareTo(bufPtr, size, largePtr, largeSize));
    }
    UnsafeAccess.free(bufPtr);
    UnsafeAccess.free(largePtr);
  }

  @Test
  public void testListSerDe() {
    log.debug("Test List serializer {}", getParameters());

    // Register LIST deallocator
    Lists.registerDeallocator();
    Lists.registerSerDe();
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }

    log.debug("Before BSM.dispose:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();

    UnsafeAccess.mallocStats.printStats();

    log.debug("Taking snapshot");
    map.snapshot();
    map.dispose();
    log.debug("After BSM.dispose:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();

    BigSortedMap.setStatsUpdatesDisabled(true);
    map = BigSortedMap.loadStore(0);
    BigSortedMap.setStatsUpdatesDisabled(false);
    map.syncStatsToGlobal();

    log.debug("Load snapshot");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();

    UnsafeAccess.mallocStats.printStats();
    long buffer = UnsafeAccess.malloc(valueSize);
    for (int i = 0; i < n; i++) {
      int size = Lists.RPOP(map, key.address, key.length, buffer, valueSize);
      assertEquals(valueSize, size);
      assertEquals(
          0, Utils.compareTo(buffer, valueSize, values.get(i).address, values.get(i).length));
    }
    UnsafeAccess.free(buffer);
  }

  @Test
  public void testRPUSHX() {
    log.debug("Test RPUSHX {}", getParameters());
    Key key = getKey();

    // Try LPUSHX - no key yet
    Value v = values.get(0);
    long[] elemPtrs = new long[] {v.address};
    int[] elemSizes = new int[] {v.length};
    long len = Lists.RPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
    // Can not add
    assertEquals(-1, (int) len);

    // Add first element
    len = Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    assertEquals(1, (int) len);

    // Now we are ready to go
    for (int i = 1; i < n; i++) {
      v = values.get(i);
      elemPtrs = new long[] {v.address};
      elemSizes = new int[] {v.length};
      len = Lists.RPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }

    log.debug(
        "Total allocated memory ={} for {} {}  byte values. Overhead={} bytes per value",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valueSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valueSize);
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      int sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      v = values.get(n - 1 - i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLPUSHX() {
    log.debug("Test LPUSHX {}", getParameters());
    Key key = getKey();

    // Try LPUSHX - no key yet
    Value v = values.get(0);
    long[] elemPtrs = new long[] {v.address};
    int[] elemSizes = new int[] {v.length};
    long len = Lists.LPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
    // Can not add
    assertEquals(-1, (int) len);

    // Add first element
    len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    assertEquals(1, (int) len);

    // Now we are ready to go
    for (int i = 1; i < n; i++) {
      v = values.get(i);
      elemPtrs = new long[] {v.address};
      elemSizes = new int[] {v.length};
      len = Lists.LPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valueSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valueSize);
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      int sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      v = values.get(n - 1 - i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLPUSHLPOP() {
    log.debug("Test LPUSHLPOP {}", getParameters());
    Key key = getKey();

    for (int i = 0; i < n; i++) {
      // log.debug(i);
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valueSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valueSize);
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      int sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      Value v = values.get(n - 1 - i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testRPUSHRPOP() {
    log.debug("Test RPUSHRPOP {}", getParameters());
    Key key = getKey();

    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      int sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      Value v = values.get(n - 1 - i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLPUSHRPOP() {
    log.debug("Test LPUSHRPOP {}", getParameters());
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      int sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      Value v = values.get(i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(valueSize, sz);
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLRMIX() {
    log.debug("Test LRMIX {}", getParameters());
    Key key = getKey();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("SEED={}", seed);
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      if (r.nextBoolean()) {
        Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      } else {
        Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      }
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      long sz;
      if (r.nextBoolean()) {
        sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      } else {
        sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      }
      assertEquals(valueSize, (int) sz);
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testRPUSHLPOP() {
    log.debug("Test RPUSHLPOP {}", getParameters());
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      int sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      Value v = values.get(i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLPUSHLINDEX() {
    log.debug("Test LPUSHLINDEX {}", getParameters());
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      Value v = values.get(n - i - 1);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(valueSize, sz);
    }
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    long end = System.currentTimeMillis();
    log.debug("Time to index {} from {} long list={}ms", n, n, end - start);
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLindexEdgeCases() {
    log.debug("Test LINDEX edge cases {}", getParameters());
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    long start = System.currentTimeMillis();
    for (int i = 1; i <= n; i++) {
      int sz = Lists.LINDEX(map, key.address, key.length, -i, buffer, bufferSize);
      Value v = values.get(i - 1);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
      assertEquals(valueSize, sz);
    }
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    // check -n -1
    int sz = Lists.LINDEX(map, key.address, key.length, -n - 1, buffer, bufferSize);
    assertEquals(-1, sz);
    // Check n
    sz = Lists.LINDEX(map, key.address, key.length, n, buffer, bufferSize);
    assertEquals(-1, sz);

    long end = System.currentTimeMillis();
    log.debug("Time to index {} from {} long list={}ms", n, n, end - start);
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testRPUSHLINDEX() {
    log.debug("Test RPUSHLINDEX {}", getParameters());
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }

    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));

    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      assertEquals(valueSize, sz);
      Value v = values.get(i);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
    }
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    long end = System.currentTimeMillis();
    log.debug("Time to index {} from {} long list={}ms", n, n, end - start);
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLMOVE() {
    log.debug("Test LMOVE {}", getParameters());
    Key key = getKey();
    Key key2 = getAnotherKey();

    // 1. Non-existent source LEFT-LEFT
    int size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.LEFT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(-1, size);

    // 2. Non-existent source LEFT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.LEFT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(-1, size);

    // 3. Non-existent source RIGHT-LEFT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.RIGHT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(-1, size);

    // 4. Non-existent source RIGHT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.RIGHT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(-1, size);

    // Push 4 values to the source
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz =
          (int)
              Lists.LPUSH(
                  map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, sz);
    }
    /*
     * SRC order of elements: 3,2,1,0
     */

    // Now repeat

    // 1. existent source LEFT-LEFT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.LEFT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 3
    Value v = values.get(3);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 3
    // SRC = 2, 1, 0
    // 2. existent source LEFT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.LEFT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 2
    v = values.get(2);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 3, 2
    // SRC = 1, 0
    // 3. existent source RIGHT-LEFT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.RIGHT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 0
    v = values.get(0);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 0, 3, 2
    // SRC = 1
    // 4. existent source RIGHT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key2.address,
            key2.length,
            Side.RIGHT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 0, 3, 2, 1
    // SRC = empty

    assertEquals(0L, Lists.LLEN(map, key.address, key.length));
    assertEquals(4L, Lists.LLEN(map, key2.address, key2.length));

    // Now repeat from dst -> src

    // 1. existent source LEFT-LEFT
    size =
        Lists.LMOVE(
            map,
            key2.address,
            key2.length,
            key.address,
            key.length,
            Side.LEFT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 0
    v = values.get(0);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 3, 2, 1
    // SRC = 0
    // 2. existent source LEFT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key2.address,
            key2.length,
            key.address,
            key.length,
            Side.LEFT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 3
    v = values.get(3);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 2, 1
    // SRC = 0, 3
    // 3. existent source RIGHT-LEFT
    size =
        Lists.LMOVE(
            map,
            key2.address,
            key2.length,
            key.address,
            key.length,
            Side.RIGHT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 2
    // SRC = 1, 0, 3
    // 4. existent source RIGHT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key2.address,
            key2.length,
            key.address,
            key.length,
            Side.RIGHT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 2
    v = values.get(2);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 1, 0, 3, 2
    assertEquals(4L, Lists.LLEN(map, key.address, key.length));
    assertEquals(0L, Lists.LLEN(map, key2.address, key2.length));

    // Same key

    // 1. existent source - source LEFT-LEFT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key.address,
            key.length,
            Side.LEFT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 1, 0, 3, 2
    // 2. existent source - source LEFT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key.address,
            key.length,
            Side.LEFT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 0, 3, 2, 1
    // 3. existent source-source RIGHT-LEFT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key.address,
            key.length,
            Side.RIGHT,
            Side.LEFT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 1, 0, 3, 2
    // 4. existent source-source RIGHT-RIGHT
    size =
        Lists.LMOVE(
            map,
            key.address,
            key.length,
            key.address,
            key.length,
            Side.RIGHT,
            Side.RIGHT,
            buffer,
            bufferSize);
    assertEquals(valueSize, size);
    // we moved element 2
    v = values.get(2);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    assertEquals(4L, Lists.LLEN(map, key.address, key.length));
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
    Lists.DELETE(map, key2.address, key2.length);
    assertEquals(0, (int) Lists.LLEN(map, key2.address, key2.length));
    // Dispose additional key
    UnsafeAccess.free(key2.address);
  }

  @Test
  public void testRPOPLPUSH() {
    log.debug("Test RPOPLPUSH {}", getParameters());
    Key key = getKey();
    Key key2 = getAnotherKey();

    int size =
        Lists.RPOPLPUSH(
            map, key.address, key.length, key2.address, key2.length, buffer, bufferSize);
    assertEquals(-1, size);

    // Push 4 values to the source
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz =
          (int)
              Lists.LPUSH(
                  map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, sz);
    }
    // Now we have SRC = 3, 2, 1, 0
    // Rotate single list
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz =
          Lists.RPOPLPUSH(
              map, key.address, key.length, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
    }

    // Rotate two lists
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz =
          Lists.RPOPLPUSH(
              map, key.address, key.length, key2.address, key2.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
    }
    assertEquals(0L, Lists.LLEN(map, key.address, key.length));
    assertEquals(4L, Lists.LLEN(map, key2.address, key2.length));

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
    Lists.DELETE(map, key2.address, key2.length);
    assertEquals(0, (int) Lists.LLEN(map, key2.address, key2.length));
    // Dispose additional key
    UnsafeAccess.free(key2.address);
  }

  @Ignore
  @Test
  public void testBLMOVE() {
    log.debug("BLMOVE - TODO");
    testLMOVE();
  }

  @Ignore
  @Test
  public void testBRPOPLPUSH() {
    log.debug("BRPOPLPUSH - TODO");
    testRPOPLPUSH();
  }

  @Test
  public void testLSET() {
    log.debug("Test LSET operation {}", getParameters());
    Key key = getKey();
    // load half of values
    int toLoad = n / 2;
    for (int i = 0; i < toLoad; i++) {
      Value v = values.get(i);
      int len =
          (int)
              Lists.LPUSH(
                  map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }

    // Now overwrite
    for (int i = toLoad; i < n; i++) {
      Value v = values.get(i);
      int len = (int) Lists.LSET(map, key.address, key.length, i - toLoad, v.address, v.length);
      assertEquals(toLoad, len);
    }

    // Verify
    for (int i = 0; i < toLoad; i++) {
      Value v = values.get(i + toLoad);
      int sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLINSERT() {
    log.debug("Test LINSERT operation {}", getParameters());
    Key key = getKey();
    // load half of values
    for (int i = 0; i < n / 2; i++) {
      Value v = values.get(i);
      int len =
          (int)
              Lists.LPUSH(
                  map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }

    // Test edge case first
    Value v = values.get(n / 2);
    long ls =
        Lists.LINSERT(map, key.address, key.length, true, buffer, bufferSize, v.address, v.length);
    assertEquals(-1L, ls);

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);
    // Insert and Verify
    int listSize = n / 2;
    for (int i = n / 2; i < n / 2 + 10000; i++) {
      int index = i;
      int insertPos = r.nextInt(listSize);
      boolean after = r.nextBoolean();
      int sz = Lists.LINDEX(map, key.address, key.length, insertPos, buffer, bufferSize);
      assertEquals(valueSize, sz);

      Value insert = values.get(index);
      ls =
          Lists.LINSERT(
              map, key.address, key.length, after, buffer, sz, insert.address, insert.length);
      listSize++;
      assertEquals((int) ls, listSize);

      sz =
          Lists.LINDEX(
              map, key.address, key.length, after ? insertPos + 1 : insertPos, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(insert.address, insert.length, buffer, sz));
      if (i % 1000 == 0) {
        log.debug(i);
      }
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }

  @Test
  public void testLRANGE() {
    log.debug("Test LRANGE operation {}", getParameters());
    Key key = getKey();
    // No exists yet

    long sz = Lists.LRANGE(map, key.address, key.length, 0, 1, buffer, bufferSize);
    assertEquals(-1L, sz);
    // load  values
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      int len =
          (int)
              Lists.RPUSH(
                  map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    // Test edge cases
    // 1. start >= n
    sz = Lists.LRANGE(map, key.address, key.length, n, n + 1, buffer, bufferSize);
    assertEquals(0L, sz);
    sz = Lists.LRANGE(map, key.address, key.length, -n - 1, -n - 1, buffer, bufferSize);
    assertEquals(0L, sz);
    // 1. start > end
    sz = Lists.LRANGE(map, key.address, key.length, 1, 0, buffer, bufferSize);
    assertEquals(0L, sz);

    // Test limited buffer

    int expNumber = (bufferSize - Utils.SIZEOF_INT) / (valueSize + Utils.sizeUVInt(valueSize));

    for (int i = 0; i < 1000; i++) {
      int i1 = r.nextInt(n);
      int i2 = r.nextInt(n);
      int start = Math.min(i1, i2);
      int end = Math.max(i1, i2);
      // TODO validate sz?
      sz = Lists.LRANGE(map, key.address, key.length, start, end, buffer, bufferSize);
      end = Math.min(end, start + expNumber - 1);
      verifyRange(start, end, buffer, bufferSize);
    }

    int largeBufferSize = Utils.SIZEOF_INT + n * (valueSize + Utils.sizeUVInt(valueSize));
    long largeBuffer = UnsafeAccess.malloc(largeBufferSize);

    // Test large buffer

    for (int i = 0; i < 1000; i++) {
      int i1 = r.nextInt(n);
      int i2 = r.nextInt(n);
      int start = Math.min(i1, i2);
      int end = Math.max(i1, i2);
      // TODO validate sz
      sz = Lists.LRANGE(map, key.address, key.length, start, end, largeBuffer, largeBufferSize);
      verifyRange(start, end, largeBuffer, largeBufferSize);
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
    UnsafeAccess.free(largeBuffer);
  }

  /**
   * Verifies range of values
   *
   * @param start start index (inclusive)
   * @param stop stop index (inclusive)
   * @param buffer buffer
   * @param bufferSize size
   */
  private static void verifyRange(int start, int stop, long buffer, int bufferSize) {

    int total = UnsafeAccess.toInt(buffer);
    assertEquals(stop - start + 1, total);
    long ptr = buffer + Utils.SIZEOF_INT;

    for (int i = start; i <= stop; i++) {
      Value v = values.get(i);
      int sz = Utils.readUVInt(ptr);
      int ssz = Utils.sizeUVInt(sz);
      assertEquals(sz, v.length);
      assertEquals(0, Utils.compareTo(v.address, v.length, ptr + ssz, sz));
      ptr += sz + ssz;
    }
  }

  @Test
  public void testLREM() {
    log.debug("Test LREM operation {}", getParameters());
    Key key = getKey();
    // No exists yet
    long removed =
        Lists.LREM(map, key.address, key.length, 0, values.get(0).address, values.get(0).length);
    assertEquals(0L, removed);

    removed =
        Lists.LREM(map, key.address, key.length, 10, values.get(0).address, values.get(0).length);
    assertEquals(0L, removed);

    removed =
        Lists.LREM(map, key.address, key.length, -10, values.get(0).address, values.get(0).length);
    assertEquals(0L, removed);

    // load  values (n-1)
    for (int i = 0; i < n - 1; i++) {
      Value v = values.get(i);
      int len =
          (int)
              Lists.RPUSH(
                  map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    // Get the last one
    Value v = values.get(n - 1);
    // Remove direct
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int) indexes[j]);
        long size =
            Lists.LINSERT(
                map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, toInsert, v.address, v.length);

      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        log.debug("direct ={}", i);
      }
    }

    // Remove direct more
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int) indexes[j]);
        long size =
            Lists.LINSERT(
                map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, 100, v.address, v.length);

      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        log.debug("direct more={}", i);
      }
    }

    // Remove reverse
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int) indexes[j]);
        long size =
            Lists.LINSERT(
                map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all

      removed = Lists.LREM(map, key.address, key.length, -toInsert, v.address, v.length);

      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        log.debug("reverse ={}", i);
      }
    }

    // Remove reverse more
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int) indexes[j]);
        long size =
            Lists.LINSERT(
                map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, -100, v.address, v.length);
      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        log.debug("reverse more={}", i);
      }
    }

    // Remove all
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int) indexes[j]);
        long size =
            Lists.LINSERT(
                map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, 0, v.address, v.length);
      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        log.debug("all ={}", i);
      }
    }

    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
  }
}
