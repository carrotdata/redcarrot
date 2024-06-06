/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.CarrotCoreBase;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.KeyValue;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;
import com.carrotdata.redcarrot.util.Value;
import org.junit.*;

public class HashesTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(HashesTest.class);

  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  int valSize = 8;
  long n;
  List<Value> values;

  public HashesTest(Object c) {
    super(c);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();

    long n = memoryDebug ? 1000000 : 2000000;

    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    values = getValues(n);
  }

  @Override
  public void extTearDown() {
    if (key != null) {
      UnsafeAccess.free(key.address);
      key = null;
    }
    for (Value v : values) {
      UnsafeAccess.free(v.address);
    }
    UnsafeAccess.free(buffer);
  }

  private List<Value> getValues(long n) {
    List<Value> values = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("VALUES SEED={}", seed);
    byte[] buf = new byte[valSize];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, valSize);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }

  private List<KeyValue> getKeyValues(int n) {
    List<KeyValue> list = new ArrayList<>(n);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("VALUES SEED={}", seed);
    byte[] buf = new byte[valSize];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long fptr = UnsafeAccess.malloc(valSize);
      long vptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, fptr, valSize);
      UnsafeAccess.copy(buf, 0, vptr, valSize);
      list.add(new KeyValue(fptr, valSize, vptr, valSize));
    }
    return list;
  }

  private Key getKey() {
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

  long countRecords(BigSortedMap map) {
    return map.countRecords();
  }

  @Ignore
  @Test
  public void testMultiSet() {

    List<KeyValue> list = getKeyValues(1000);
    List<KeyValue> copy = new ArrayList<>(list.size());
    copy.addAll(list);

    Key key = getKey();
    int nn = Hashes.HSET(map, key.address, key.length, copy);
    assertEquals(list.size(), nn);
    assertEquals(list.size(), (int) Hashes.HLEN(map, key.address, key.length));
    // Verify inserted
    long buffer = UnsafeAccess.malloc(valSize * 2L);
    for (KeyValue kv : list) {
      int result = Hashes.HEXISTS(map, key.address, key.length, kv.keyPtr, kv.keySize);
      assertEquals(1, result);
      int size =
          Hashes.HGET(map, key.address, key.length, kv.keyPtr, kv.keySize, buffer, valSize * 2);
      assertEquals(valSize, size);
      assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, valSize));
    }
    map.dispose();
    UnsafeAccess.free(buffer);
    UnsafeAccess.free(key.address);
    list.forEach(x -> {
      UnsafeAccess.free(x.keyPtr);
      UnsafeAccess.free(x.valuePtr);
    });
  }

  @Test
  public void testSetExists() {

    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize, elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    log.debug(
      "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
      BigSortedMap.getGlobalAllocatedMemory(), n, keySize + valSize,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n - keySize - valSize, end - start);

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int res =
          Hashes.HEXISTS(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    log.debug("Time exist={}ms", end - start);
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) countRecords(map));
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
  }

  @Test
  public void testNullValues() {

    Key key = getKey();
    long NULL = UnsafeAccess.malloc(1);
    String[] fields = new String[] { "f1", "f2", "f3", "f4" };
    String[] values = new String[] { "v1", null, "v3", null };

    for (int i = 0; i < fields.length; i++) {
      String f = fields[i];
      String v = values[i];
      long fPtr = UnsafeAccess.allocAndCopy(f, 0, f.length());
      int fSize = f.length();
      long vPtr = v == null ? NULL : UnsafeAccess.allocAndCopy(v, 0, v.length());
      int vSize = v == null ? 0 : v.length();
      Hashes.HSET(map, key.address, key.length, fPtr, fSize, vPtr, vSize);
    }

    long buffer = UnsafeAccess.malloc(8);

    for (int i = 0; i < fields.length; i++) {
      String f = fields[i];
      String v = values[i];
      long fPtr = UnsafeAccess.allocAndCopy(f, 0, f.length());
      int fSize = f.length();
      long vPtr = v == null ? NULL : UnsafeAccess.allocAndCopy(v, 0, v.length());
      int vSize = v == null ? 0 : v.length();
      int size = Hashes.HGET(map, key.address, key.length, fPtr, fSize, buffer, 8);

      if (size < 0) { // does not exists
        log.fatal("field not found {}", f);
        System.exit(-1);
      }

      if (vPtr == NULL && size != 0) {
        log.fatal("Expected NULL for {}", f);
        System.exit(-1);
      }

      if (vPtr == NULL) {
        log.debug("Found NULL for {} size={}", f, size);
      }

      if (Utils.compareTo(vPtr, vSize, buffer, size) != 0) {
        log.fatal("Failed for {}", f);
        System.exit(-1);
      }
    }

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
  }

  @Test
  public void testSetGet() {

    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize, elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    log.debug(
      "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
      BigSortedMap.getGlobalAllocatedMemory(), n, keySize + valSize,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n - (keySize + valSize), end - start);

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    long buffer = UnsafeAccess.malloc(2L * valSize);
    int bufferSize = 2 * valSize;

    for (int i = 0; i < n; i++) {
      int size = Hashes.HGET(map, key.address, key.length, values.get(i).address,
        values.get(i).length, buffer, bufferSize);
      assertEquals(values.get(i).length, size);
      assertEquals(0, Utils.compareTo(values.get(i).address, values.get(i).length, buffer, size));
    }

    end = System.currentTimeMillis();
    log.debug("Time get={}ms", end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) countRecords(map));
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    UnsafeAccess.free(buffer);
  }

  @Test
  public void testAddRemove() {

    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize, elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    log.debug(
      "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
      BigSortedMap.getGlobalAllocatedMemory(), n, keySize + valSize,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n - (keySize + valSize), end - start);
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    BigSortedMap.printGlobalMemoryAllocationStats();

    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int res =
          Hashes.HDEL(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    log.debug("Time to delete={}ms", end - start);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    long recc = countRecords(map);
    assertEquals(0, (int) recc);
  }

  @Test
  public void testAddRemoveMulti() {

    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, elemPtr, elemSize, elemPtr, elemSize, elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    log.debug(
      "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
      BigSortedMap.getGlobalAllocatedMemory(), n, keySize + valSize,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n - (keySize + valSize), end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    start = System.currentTimeMillis();

    for (int i = 0; i < n; i++) {
      boolean res = Hashes.DELETE(map, values.get(i).address, values.get(i).length);
      assertTrue(res);
    }

    end = System.currentTimeMillis();
    log.debug("Time to delete={}ms", end - start);
    long recc = countRecords(map);
    log.debug("Map.size ={}", recc);
    assertEquals(0, (int) recc);
  }
}
