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

import java.io.IOException;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.Ignore;
import org.junit.Test;

public class BigSortedMapSnapshotTest {

  private static final Logger log = LogManager.getLogger(BigSortedMapSnapshotTest.class);

  BigSortedMap map;
  long totalLoaded;
  long MAX_ROWS = 1000000;
  int EXT_VALUE_SIZE = 4096;
  int EXT_KEY_SIZE = 4096;
  int KEY_SIZE = 10;
  long seed1 = System.currentTimeMillis();
  long seed2 = System.currentTimeMillis() + 1;

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

  /** Verify normal records with type = EMBEDDED */
  void verifyRecords() {

    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + i).getBytes();
      byte[] value = ("VALUE" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, valPtr, value.length);
      boolean result = map.exists(keyPtr, key.length);
      assertTrue(result);
      UnsafeAccess.free(keyPtr);
      UnsafeAccess.free(valPtr);
    }
  }

  /**
   * Verify records with external value allocations
   *
   * @param n number of records to verify
   */
  void verifyExtValueRecords(int n) {
    Random rnd = new Random(seed2);
    byte[] key = new byte[KEY_SIZE];
    long keyPtr = UnsafeAccess.malloc(KEY_SIZE);
    long buf = UnsafeAccess.malloc(EXT_VALUE_SIZE);

    for (int i = 0; i < n; i++) {
      rnd.nextBytes(key);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean result = map.exists(keyPtr, key.length);
      assertTrue(result);
      long size = map.get(keyPtr, key.length, buf, EXT_VALUE_SIZE, 0);
      assertEquals(EXT_VALUE_SIZE, (int) size);
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buf);
  }

  /**
   * Verify records with external key-value allocations
   *
   * @param n number of records to verify
   */
  void verifyExtKeyValueRecords(int n) {
    Random rnd = new Random(seed1);
    byte[] key = new byte[EXT_KEY_SIZE];
    long keyPtr = UnsafeAccess.malloc(EXT_KEY_SIZE);
    long buf = UnsafeAccess.malloc(EXT_VALUE_SIZE);

    for (int i = 0; i < n; i++) {
      rnd.nextBytes(key);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean result = map.exists(keyPtr, key.length);
      assertTrue(result);
      long size = map.get(keyPtr, key.length, buf, EXT_VALUE_SIZE, 0);
      assertEquals(EXT_VALUE_SIZE, (int) size);
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buf);
  }

  /**
   * Load regular record
   *
   * @param totalLoaded
   * @return
   */
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

  /**
   * Load records with external key-value allocations
   *
   * @param num number of records to load
   */
  void loadExtKeyValueRecords(int num) {
    Random rnd = new Random(seed1);
    byte[] key = new byte[EXT_KEY_SIZE];
    long keyPtr = UnsafeAccess.malloc(EXT_KEY_SIZE);
    byte[] value = new byte[EXT_VALUE_SIZE];
    Random rr = new Random(10);
    rr.nextBytes(value);
    long buf = UnsafeAccess.malloc(EXT_VALUE_SIZE);
    UnsafeAccess.copy(value, 0, buf, EXT_VALUE_SIZE);
    for (int i = 0; i < num; i++) {
      rnd.nextBytes(key);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean result = map.put(keyPtr, key.length, buf, EXT_VALUE_SIZE, 0);
      assertTrue(result);
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buf);
  }

  /**
   * Load records with external value allocations
   *
   * @param num number of records to load
   */
  void loadExtValueRecords(int num) {
    Random rnd = new Random(seed2);
    byte[] key = new byte[10];
    long keyPtr = UnsafeAccess.malloc(key.length);
    byte[] value = new byte[EXT_VALUE_SIZE];
    Random rr = new Random(10);
    rr.nextBytes(value);
    long buf = UnsafeAccess.malloc(EXT_VALUE_SIZE);
    UnsafeAccess.copy(value, 0, buf, EXT_VALUE_SIZE);
    for (int i = 0; i < num; i++) {
      rnd.nextBytes(key);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean result = map.put(keyPtr, key.length, buf, EXT_VALUE_SIZE, 0);
      assertTrue(result);
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buf);
  }

  private void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(10000000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while (totalLoaded < MAX_ROWS) {
      totalLoaded++;
      load(totalLoaded);
      if (totalLoaded % 1000000 == 0) {
        log.debug("Loaded {}", totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to load= {} ={}ma", totalLoaded, end - start);
    long scanned = countRecords();
    start = System.currentTimeMillis();
    log.debug("Scanned={} in {}ms", scanned, start - end);
    log.debug("\nTotal memory       ={}", BigSortedMap.getGlobalAllocatedMemory());
    log.debug("Total   data       ={}", BigSortedMap.getGlobalDataSize());
    log.debug("Compressed size    ={}", BigSortedMap.getGlobalCompressedDataSize());
    log.debug(
        "Compression  ratio ={}",
        (float) BigSortedMap.getGlobalDataSize() / BigSortedMap.getGlobalAllocatedMemory());
    log.debug("");
    assertEquals(totalLoaded, scanned);
  }

  private void tearDown() {
    map.dispose();
    log.debug("Memory stat after teardown:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    UnsafeAccess.mallocStats();
  }

  private void allTests() throws IOException {
    setUp();
    testSnapshotNoExternalNoCustomAllocations();
    tearDown();
    setUp();
    testSnapshotWithExternalNoCustomAllocations();
    tearDown();
  }

  // @Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 1; i++) {
      log.debug("\n********* {} ********** Codec = NONE\n", i);
      allTests();
      UnsafeAccess.mallocStats();
    }
  }

  // @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    for (int i = 0; i < 1; i++) {
      log.debug("\n********* {} ********** Codec = LZ4\n", i);
      allTests();
      UnsafeAccess.mallocStats();
    }
  }

  @Ignore
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    for (int i = 0; i < 1; i++) {
      log.debug("\n********* {} ********** Codec = LZ4HC\n", i);
      allTests();
      UnsafeAccess.mallocStats();
    }
  }

  @Ignore
  @Test
  public void testSnapshotNoExternalNoCustomAllocations() throws IOException {
    log.debug("\nTestSnapshotNoExteranlNoCustomAllocations");
    long start = System.currentTimeMillis();
    map.snapshot();
    long end = System.currentTimeMillis();
    log.debug("snapshot create={}ms", end - start);
    log.debug("\nAfter snapshot:");
    BigSortedMap.printGlobalMemoryAllocationStats();

    map.dispose();
    log.debug("\nAfter dispose:");
    BigSortedMap.printGlobalMemoryAllocationStats();

    start = System.currentTimeMillis();

    // This is how we load data from a snapshot
    // 1. Disable global stats updates
    // 2. Load local store
    // 3. enable global stats update
    // sync stats from local to global

    BigSortedMap.setStatsUpdatesDisabled(true);
    map = BigSortedMap.loadStore(0);

    BigSortedMap.setStatsUpdatesDisabled(false);
    map.syncStatsToGlobal();

    end = System.currentTimeMillis();
    log.debug("snapshot load={}ms", end - start);
    log.debug("\nAfter load:");
    map.printMemoryAllocationStats();
    BigSortedMap.printGlobalMemoryAllocationStats();

    start = System.currentTimeMillis();
    long records = countRecords();
    end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", records, end - start);
    assertEquals(totalLoaded, records);

    start = System.currentTimeMillis();
    verifyRecords();
    end = System.currentTimeMillis();
    log.debug("Verified {} in {}ms", records, end - start);
  }

  @Ignore
  @Test
  public void testSnapshotWithExternalNoCustomAllocations() throws IOException {
    log.debug("\nTestSnapshotWithExternalNoCustomAllocations");
    int extValueLoaded = Math.max(1, (int) (MAX_ROWS / 100)); // 1% of rows are with external value
    int extKeyValueLoaded =
        Math.max(1, (int) (MAX_ROWS / 100)); // 1% of rows with external key-value
    // Load records with external values
    loadExtValueRecords(extValueLoaded);
    // Load records with external key-values
    loadExtKeyValueRecords(extKeyValueLoaded);
    int extraLoaded = extValueLoaded + extKeyValueLoaded;

    long start = System.currentTimeMillis();
    map.snapshot();
    long end = System.currentTimeMillis();
    log.debug("snapshot create={}ms", end - start);
    log.debug("\nAfter snapshot:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();

    map.dispose();
    log.debug("\nAfter dispose:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();

    start = System.currentTimeMillis();
    // This is how we load data from a snapshot
    // 1. Disable global stats updates
    // 2. Load local store
    // 3. enable global stats update
    // sync stats from local to global

    BigSortedMap.setStatsUpdatesDisabled(true);
    map = BigSortedMap.loadStore(0);
    BigSortedMap.setStatsUpdatesDisabled(false);
    map.syncStatsToGlobal();

    end = System.currentTimeMillis();
    log.debug("snapshot load={}ms", end - start);
    log.debug("\nAfter load:");
    BigSortedMap.printGlobalMemoryAllocationStats();
    map.printMemoryAllocationStats();
    start = System.currentTimeMillis();
    long records = countRecords();
    end = System.currentTimeMillis();
    log.debug("Scanned {} in {}ms", records, end - start);
    assertEquals(totalLoaded + extraLoaded, records);

    start = System.currentTimeMillis();
    verifyRecords();
    verifyExtValueRecords(extValueLoaded);
    verifyExtKeyValueRecords(extKeyValueLoaded);
    end = System.currentTimeMillis();
    log.debug("Verified {} in {}ms", records, end - start);
  }
}
