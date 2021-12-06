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
package org.bigbase.carrot.redis.hashes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.CarrotCoreBase;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.KeyValue;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HashScannerTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(HashScannerTest.class);

  int valSize = 8;
  int fieldSize = 8;
  long n = 100000L;

  public HashScannerTest(Object c) {
    super(c);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
  }

  @Override
  public void extTearDown() {}

  private List<KeyValue> getKeyValues(long n) {
    List<KeyValue> values = new ArrayList<>();

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("VALUES SEED={}", seed);
    byte[] vbuf = new byte[valSize];
    byte[] fbuf = new byte[fieldSize];
    for (int i = 0; i < n; i++) {
      r.nextBytes(fbuf);
      r.nextBytes(vbuf);
      long vptr = UnsafeAccess.malloc(valSize);
      long fptr = UnsafeAccess.malloc(fieldSize);
      UnsafeAccess.copy(vbuf, 0, vptr, vbuf.length);
      UnsafeAccess.copy(fbuf, 0, fptr, fbuf.length);
      values.add(new KeyValue(fptr, fieldSize, vptr, valSize));
    }
    return values;
  }

  private Key getKey() {
    long ptr = UnsafeAccess.malloc(valSize);
    byte[] buf = new byte[valSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("KEY SEED={}", seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, valSize);
    return new Key(ptr, valSize);
  }

  private void loadData(Key key, List<KeyValue> values) {
    int expected = values.size();
    int loaded = Hashes.HSET(map, key, values);
    assertEquals(expected, loaded);
  }

  @Test
  public void testSingleFullScanner() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();

    loadData(key, values);

    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {} byte field-values. Overhead={} bytes per value. Time to load:{} ",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      /*DEBUG*/ log.debug("Set size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      HashScanner scanner =
          Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, false);
      int expected = copy.size();
      if (scanner == null) {
        assertEquals(0, expected);
        break;
      }
      int cc = 0;
      while (scanner.hasNext()) {
        cc++;
        scanner.next();
      }
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testEdgeConditions() throws IOException {

    byte[] zero1 = new byte[] {0};
    byte[] zero2 = new byte[] {0, 0};
    byte[] max1 =
        new byte[] {
          (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
          (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0
        };
    byte[] max2 =
        new byte[] {
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff
        };
    long zptr1 = UnsafeAccess.allocAndCopy(zero1, 0, zero1.length);
    int zptrSize1 = zero1.length;
    long zptr2 = UnsafeAccess.allocAndCopy(zero2, 0, zero2.length);
    int zptrSize2 = zero2.length;
    long mptr1 = UnsafeAccess.allocAndCopy(max1, 0, max1.length);
    int mptrSize1 = max1.length;
    long mptr2 = UnsafeAccess.allocAndCopy(max2, 0, max2.length);
    int mptrSize2 = max2.length;

    log.debug("Test edge conditions {} elements", n);
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();
    loadData(key, copy);
    long end = System.currentTimeMillis();

    Utils.sortKeyValues(values);

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize,
        end - start);

    // Direct
    HashScanner scanner =
        Hashes.getScanner(
            map, key.address, key.length, zptr1, zptrSize1, zptr2, zptrSize2, false, false);
    assertNotNull(scanner);
    assertFalse(scanner.hasNext());
    scanner.close();

    // Reverse
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, zptr1, zptrSize1, zptr2, zptrSize2, false, true);
    assertNull(scanner);

    // Direct
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, mptr1, mptrSize1, mptr2, mptrSize2, false, false);
    assertNotNull(scanner);
    assertFalse(scanner.hasNext());
    scanner.close();

    // Reverse
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, mptr1, mptrSize1, mptr2, mptrSize2, false, true);
    assertNull(scanner);

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    int index = r.nextInt(values.size());
    int expected = index;
    KeyValue v = values.get(index);
    // Direct
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, zptr1, zptrSize1, v.keyPtr, v.keySize, false, false);

    if (expected == 0) {
      assertNotNull(scanner);
      assertFalse(scanner.hasNext());
    } else {
      assertEquals(expected, Utils.count(scanner));
    }
    assertNotNull(scanner);
    scanner.close();

    // Reverse
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, zptr1, zptrSize1, v.keyPtr, v.keySize, false, true);

    if (expected == 0) {
      assertNull(scanner);
    } else {
      assertEquals(expected, Utils.countReverse(scanner));
      assertNotNull(scanner);
      scanner.close();
    }
    // Always close ALL scanners

    index = r.nextInt(values.size());
    expected = values.size() - index;
    v = values.get(index);
    // Direct
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, v.keyPtr, v.keySize, mptr2, mptrSize2, false, false);

    if (expected == 0) {
      assertNotNull(scanner);
      assertFalse(scanner.hasNext());
    } else {
      assertEquals(expected, Utils.count(scanner));
    }
    assertNotNull(scanner);
    scanner.close();

    // Reverse
    scanner =
        Hashes.getScanner(
            map, key.address, key.length, v.keyPtr, v.keySize, mptr2, mptrSize2, false, true);

    if (expected == 0) {
      assertNull(scanner);
    } else {
      assertEquals(expected, Utils.countReverse(scanner));
      assertNotNull(scanner);
      scanner.close();
    }

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSingleFullScannerReverse() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();

    loadData(key, values);

    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {} byte field-values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      log.debug("Set size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      HashScanner scanner =
          Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, true);

      int expected = copy.size();
      if (scanner == null && expected == 0) {
        break;
      } else if (scanner == null) {
        fail("Scanner is null, but expected=" + expected);
      }
      int cc = 0;
      do {
        cc++;
      } while (scanner.previous());
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSinglePartialScanner() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    Utils.sortKeyValues(values);

    long start = System.currentTimeMillis();
    List<KeyValue> copy = copy(values);
    loadData(key, values);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      log.debug("Hash size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = r.nextInt(copy.size());
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;

      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = endIndex - startIndex;
      HashScanner scanner =
          Hashes.getScanner(
              map, key.address, key.length, startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        assertEquals(0, expected);
        continue;
      }
      int cc = 0;
      while (scanner.hasNext()) {
        cc++;
        scanner.next();
      }
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSinglePartialScannerOpenStart() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    Utils.sortKeyValues(values);
    List<KeyValue> copy = copy(values);

    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      log.debug("Hash size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = 0;
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;

      long startPtr = 0; // copy.get(startIndex).keyPtr;
      int startSize = 0; // copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = endIndex - startIndex;
      HashScanner scanner =
          Hashes.getScanner(
              map, key.address, key.length, startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        assertEquals(0, expected);
        continue;
      }
      int cc = 0;
      while (scanner.hasNext()) {
        cc++;
        scanner.next();
      }
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSinglePartialScannerOpenEnd() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    Utils.sortKeyValues(values);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      log.debug("Hash size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = r.nextInt(copy.size());
      int endIndex = copy.size();

      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = 0;
      int endSize = 0;

      int expected = endIndex - startIndex;
      HashScanner scanner =
          Hashes.getScanner(
              map, key.address, key.length, startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        assertEquals(0, expected);
        continue;
      }
      int cc = 0;
      while (scanner.hasNext()) {
        cc++;
        scanner.next();
      }
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSinglePartialScannerReverse() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    Utils.sortKeyValues(values);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      /*DEBUG*/ log.debug("Hash size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = r.nextInt(copy.size());
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;

      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = endIndex - startIndex;
      HashScanner scanner =
          Hashes.getScanner(
              map, key.address, key.length, startPtr, startSize, endPtr, endSize, false, true);
      if (scanner == null && expected == 0) {
        continue;
      } else if (scanner == null) {
        fail("Scanner is null, but expected=" + expected);
      }
      int cc = 0;
      do {
        cc++;
      } while (scanner.previous());
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSinglePartialScannerReverseOpenStart() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    Utils.sortKeyValues(values);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} fot {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      log.debug("Hash size={}", copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = 0; // r.nextInt(copy.size());
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;

      long startPtr = 0; // copy.get(startIndex).keyPtr;
      int startSize = 0; // copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = endIndex - startIndex;
      HashScanner scanner =
          Hashes.getScanner(
              map, key.address, key.length, startPtr, startSize, endPtr, endSize, false, true);
      if (scanner == null && expected == 0) {
        continue;
      } else if (scanner == null) {
        fail("Scanner is null, but expected=" + expected);
      }
      int cc = 0;
      do {
        cc++;
      } while (scanner.previous());
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testSinglePartialScannerReverseOpenEnd() throws IOException {

    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    Utils.sortKeyValues(values);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        fieldSize + valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - fieldSize - valSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long card;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {
      assertEquals(copy.size(), (int) card);
      /*DEBUG*/ log.debug("Hash size={}", copy.size());

      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = r.nextInt(copy.size());
      int endIndex = copy.size(); // r.nextInt(copy.size() - startIndex) + startIndex;

      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = 0; // copy.get(endIndex).keyPtr;
      int endSize = 0; // copy.get(endIndex).keySize;

      int expected = endIndex - startIndex;
      HashScanner scanner =
          Hashes.getScanner(
              map, key.address, key.length, startPtr, startSize, endPtr, endSize, false, true);
      if (scanner == null && expected == 0) {
        continue;
      } else if (scanner == null) {
        fail("Scanner is null, but expected=" + expected);
      }
      int cc = 0;
      do {
        cc++;
      } while (scanner.previous());
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testDirectScannerPerformance() throws IOException {

    int n = 5000; // 5M elements
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    List<KeyValue> copy = copy(values);
    long start = System.currentTimeMillis();
    loadData(key, copy);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize,
        end - start);

    HashScanner scanner = Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, false);

    start = System.currentTimeMillis();
    long count = 0;
    assertNotNull(scanner);
    while (scanner.hasNext()) {
      count++;
      scanner.next();
    }
    scanner.close();
    assertEquals(count, n);
    end = System.currentTimeMillis();
    log.debug("Scanned {} elements in {}ms", n, end - start);
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  @Test
  public void testReverseScannerPerformance() throws IOException {

    int n = 5000; // 5M elements
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    List<KeyValue> copy = copy(values);

    long start = System.currentTimeMillis();
    loadData(key, copy);
    long end = System.currentTimeMillis();

    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize,
        end - start);

    HashScanner scanner = Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, true);

    start = System.currentTimeMillis();
    long count = 0;

    do {
      count++;
    } while (Objects.requireNonNull(scanner).previous());
    scanner.close();
    assertEquals(count, n);

    end = System.currentTimeMillis();
    log.debug("Scanned (reversed) {} elements in {}ms", n, end - start);
    // Free memory
    UnsafeAccess.free(key.address);
    values.forEach(
        x -> {
          UnsafeAccess.free(x.keyPtr);
          UnsafeAccess.free(x.valuePtr);
        });
  }

  private <T> List<T> copy(List<T> src) {
    return new ArrayList<>(src);
  }

  private void deleteRandom(
      BigSortedMap map, long keyPtr, int keySize, List<KeyValue> copy, Random r) {
    int toDelete = copy.size() < 10 ? copy.size() : r.nextInt(copy.size() / 2);
    for (int i = 0; i < toDelete; i++) {
      int n = r.nextInt(copy.size());
      KeyValue v = copy.remove(n);
      int count = Hashes.HDEL(map, keyPtr, keySize, v.keyPtr, v.keySize);
      assertEquals(1, count);
    }
  }
}
