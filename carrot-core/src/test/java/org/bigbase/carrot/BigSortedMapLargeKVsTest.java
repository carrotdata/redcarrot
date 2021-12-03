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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class BigSortedMapLargeKVsTest extends CarrotCoreBase2 {

  private static final Logger log = LogManager.getLogger(BigSortedMapLargeKVsTest.class);

  long totalLoaded;
  List<Key> keys;

  public BigSortedMapLargeKVsTest(Object c, Object m) {
    super(c, m);
    BigSortedMap.setMaxBlockSize(4096);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();

    map.printMemoryAllocationStats();
    BigSortedMap.printGlobalMemoryAllocationStats();
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    keys = fillMap(map);
    log.debug("Loaded");
    Utils.sortKeys(keys);
    totalLoaded = keys.size();
    // Delete 20% of keys to guarantee that we will be able
    // to delete than reinsert
    List<Key> deleted = delete((int) totalLoaded / 5);
    keys.removeAll(deleted);
    // Update total loaded
    log.debug("Adjusted size by {} keys", deleted.size());
    totalLoaded -= deleted.size();
    deallocate(deleted);
    long end = System.currentTimeMillis();
    log.debug("Time to load= {} = {}ms", totalLoaded, end - start);
    verifyGets(keys);
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long scanned = verifyScanner(scanner, keys);
    scanner.close();
    log.debug("Scanned={}", scanned);
    log.debug("Total memory={}", BigSortedMap.getGlobalAllocatedMemory());
    log.debug("Total   data={}", BigSortedMap.getGlobalBlockDataSize());
    log.debug("Total  index={}", BigSortedMap.getGlobalBlockIndexSize());
    assertEquals(totalLoaded, scanned);
  }

  @Override
  public void extTearDown() {
    // Free keys
    deallocate(keys);
  }

  private void deallocate(List<Key> keys) {
    for (Key key : keys) {
      UnsafeAccess.free(key.address);
    }
    keys.clear();
  }

  @Test
  public void testDeleteUndeleted() throws IOException {

    List<Key> keys = delete(100);
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());
  }

  @Test
  public void testGetAfterLoad() {

    long start = System.currentTimeMillis();
    for (Key key : keys) {

      long valPtr = UnsafeAccess.malloc(key.length);

      try {
        long size = map.get(key.address, key.length, valPtr, key.length, Long.MAX_VALUE);
        assertEquals(key.length, (int) size);
        assertEquals(0, Utils.compareTo(key.address, key.length, valPtr, (int) size));
      } finally {
        UnsafeAccess.free(valPtr);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to get {} ={}ms", totalLoaded, end - start);
  }

  @Test
  public void testExists() {

    long start = System.currentTimeMillis();
    for (Key key : keys) {
      try {
        boolean result = map.exists(key.address, key.length);
        assertTrue(result);
      } catch (Throwable t) {
        log.error("StackTrace: ", t);
        throw t;
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to exist {} ={}ms", totalLoaded, end - start);
  }

  @Test
  public void testFullMapScanner() throws IOException {

    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long start = System.currentTimeMillis();
    long count = 0;

    while (scanner.hasNext()) {
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      long key = UnsafeAccess.malloc(keySize);
      long value = UnsafeAccess.malloc(valSize);
      scanner.key(key, keySize);
      scanner.value(value, valSize);
      Key kkey = keys.get((int) count);
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, key, keySize));
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, value, valSize));
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      count++;
      scanner.next();
    }

    long end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", count, end - start);
    assertEquals(keys.size(), (int) count);
    scanner.close();
  }

  @Test
  public void testDirectMemoryFullMapScanner() throws IOException {

    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long start = System.currentTimeMillis();
    long count = 0;

    while (scanner.hasNext()) {
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      long key = UnsafeAccess.malloc(keySize);
      long value = UnsafeAccess.malloc(valSize);
      scanner.key(key, keySize);
      scanner.value(value, valSize);
      Key kkey = keys.get((int) count);
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, key, keySize));
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, value, valSize));
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      count++;
      scanner.next();
    }

    long end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", count, end - start);
    assertEquals(keys.size(), (int) count);
    scanner.close();
  }

  @Test
  public void testDirectMemoryAllRangesMapScanner() throws IOException {

    Random r = new Random();
    int startIndex = r.nextInt((int) totalLoaded);
    int stopIndex = r.nextInt((int) totalLoaded);

    if (startIndex > stopIndex) {
      int tmp = startIndex;
      startIndex = stopIndex;
      stopIndex = tmp;
    }
    Key startKey = keys.get(startIndex);
    Key stopKey = keys.get(stopIndex);

    BigSortedMapScanner scanner = map.getScanner(0, 0, startKey.address, startKey.length);
    long count1 = countRows(scanner);
    scanner.close();
    scanner = map.getScanner(startKey.address, startKey.length, stopKey.address, stopKey.length);
    long count2 = countRows(scanner);
    scanner.close();
    scanner = map.getScanner(stopKey.address, stopKey.length, 0, 0);
    long count3 = countRows(scanner);
    scanner.close();
    assertEquals(totalLoaded, count1 + count2 + count3);
  }

  private long countRows(BigSortedMapScanner scanner) throws IOException {
    long start = System.currentTimeMillis();
    long count = 0;
    long prev = 0;
    int prevLen = 0;

    while (scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      scanner.key(key, keySize);
      if (prev != 0) {
        assertTrue(Utils.compareTo(prev, prevLen, key, keySize) < 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      scanner.next();
    }
    if (prev != 0) {
      UnsafeAccess.free(prev);
    }
    long end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", count, end - start);
    return count;
  }

  @Test
  public void testFullMapScannerWithDeletes() throws IOException {

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    int toDelete = r.nextInt((int) totalLoaded);
    log.debug("testFullMapScannerWithDeletes SEED={} toDelete={}", seed, toDelete);
    List<Key> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long start = System.currentTimeMillis();
    long count = 0;

    long prev = 0;
    int prevSize = 0;
    while (scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      scanner.key(key, keySize);
      if (prev != 0) {
        assertTrue(Utils.compareTo(prev, prevSize, key, keySize) < 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      prevSize = keySize;
      scanner.next();
    }
    if (prev > 0) {
      UnsafeAccess.free(prev);
    }

    long end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", count, end - start);
    assertEquals(totalLoaded - toDelete, count);
    scanner.close();
    undelete(deletedKeys);
  }

  @Test
  public void testDirectMemoryFullMapScannerWithDeletes() throws IOException {

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    int toDelete = r.nextInt((int) totalLoaded);
    log.debug("testDirectMemoryFullMapScannerWithDeletes SEED={} toDelete={}", seed, toDelete);
    List<Key> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long start = System.currentTimeMillis();
    long count = 0;

    long prev = 0;
    int prevSize = 0;
    while (scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      scanner.key(key, keySize);
      if (prev != 0) {
        assertTrue(Utils.compareTo(prev, prevSize, key, keySize) < 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      prevSize = keySize;
      scanner.next();
    }
    if (prev > 0) {
      UnsafeAccess.free(prev);
    }

    long end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", count, end - start);
    assertEquals(totalLoaded - toDelete, count);
    scanner.close();
    undelete(deletedKeys);
  }

  void verifyGets(List<Key> keys) {
    int counter = 0;
    for (Key key : keys) {

      if (!map.exists(key.address, key.length)) {
        fail("FAILED index=" + counter + " key length=" + key.length + " key=" + key.address);
      }
      counter++;
    }
  }

  /**
   * Delete X - Undelete X not always work, b/c our map is FULL before deletion and there is no
   * guarantee that insertion X deleted rows back will succeed
   *
   * @param keys list of keys
   */
  private void undelete(List<Key> keys) {
    int count = 1;
    for (Key key : keys) {
      count++;
      boolean res = map.put(key.address, key.length, key.address, key.length, 0);
      if (!res) {
        log.debug(
            "Count = {} total={} memory ={}",
            count,
            keys.size(),
            BigSortedMap.getGlobalAllocatedMemory());
      }
      assertTrue(res);
    }
  }

  private List<Key> delete(int num) {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Delete seed ={}", seed);
    int numDeleted = 0;
    long valPtr = UnsafeAccess.malloc(1);
    List<Key> list = new ArrayList<>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int) totalLoaded);
      Key key = keys.get(i);
      long len = map.get(key.address, key.length, valPtr, 0, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
      } else {
        boolean res = map.delete(key.address, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
      }
    }
    UnsafeAccess.free(valPtr);
    log.debug("Deleted={} collisions={}", numDeleted, collisions);
    return list;
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

  private long verifyScanner(BigSortedMapScanner scanner, List<Key> keys) throws IOException {
    int counter = 0;
    int delta = 0;
    while (scanner.hasNext()) {
      int keySize = scanner.keySize();
      if (keySize != keys.get(counter + delta).length) {
        log.debug(
            "counter={} expected key size={} found={}", counter, keys.get(counter).length, keySize);
        delta++;
      }
      long buf = UnsafeAccess.malloc(keySize);
      Key key = keys.get(counter + delta);
      scanner.key(buf, keySize);
      assertEquals(0, Utils.compareTo(buf, keySize, key.address, key.length));
      int size = scanner.value(buf, keySize);
      assertEquals(keySize, size);
      assertEquals(0, Utils.compareTo(buf, keySize, key.address, key.length));

      UnsafeAccess.free(buf);
      scanner.next();
      counter++;
    }
    return counter;
  }

  protected ArrayList<Key> fillMap(BigSortedMap map) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("FILL SEED={}", seed);
    int maxSize = 2048;
    boolean result = true;
    long total = 0;
    while (result) {
      int len = r.nextInt(maxSize - 16) + 16;
      byte[] key = new byte[len];
      r.nextBytes(key);
      key = Bytes.toHex(key).getBytes();
      len = key.length;
      long keyPtr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, keyPtr, len);
      result = map.put(keyPtr, len, keyPtr, len, 0);
      total += 2L * len;
      if (result) {
        keys.add(new Key(keyPtr, len));
      } else {
        UnsafeAccess.free(keyPtr);
      }
    }
    log.debug(
        "Loaded {} records, size={} limit={}",
        keys.size(),
        total,
        BigSortedMap.getGlobalMemoryLimit());
    return keys;
  }
}
