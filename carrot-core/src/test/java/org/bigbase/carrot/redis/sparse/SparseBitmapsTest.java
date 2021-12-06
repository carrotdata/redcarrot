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
package org.bigbase.carrot.redis.sparse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.CarrotCoreBase;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class SparseBitmapsTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(SparseBitmapsTest.class);

  Key key, key2;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  int nBits;

  public SparseBitmapsTest(Object c) {
    super(c);
  }

  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();

    nBits = 1000000; // memoryDebug ? 100000 : 1000000;
    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    key = getKey();
  }

  @Override
  public void extTearDown() {

    UnsafeAccess.free(key.address);
    if (key2 != null) {
      UnsafeAccess.free(key2.address);
      key2 = null;
    }
    UnsafeAccess.free(buffer);
  }

  @Test
  public void testSetBitGetBitLoop() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    int totalCount = 0;

    long start = System.currentTimeMillis();
    for (int i = 0; i < nBits; i++) {
      offset = Math.abs(r.nextLong() / 2);
      int oldbit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      if (oldbit == 1) {
        continue;
      }
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (bit != 0) {
        log.debug("FAILED i={} offset ={}", i, offset);
      }
      assertEquals(0, bit);
      bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      if (bit != 1) {
        log.debug("i={} offset={}", i, offset);
      }
      assertEquals(1, bit);
      totalCount++;
      if (totalCount % 10000 == 0) {
        log.debug(totalCount);
      }
    }

    long count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(totalCount, (int) count);

    /*DEBUG*/ log.debug("totalCount={} N={}", totalCount, nBits);
    /*DEBUG*/ log.debug("Total RAM={}", UnsafeAccess.getAllocatedMemory());

    BigSortedMap.printGlobalMemoryAllocationStats();

    long end = System.currentTimeMillis();

    log.debug("Time for {} new SetBit/GetBit/CountBits ={}", nBits, end - start);

    Random rr = new Random();
    rr.setSeed(seed);

    start = System.currentTimeMillis();
    for (int i = 0; i < nBits; i++) {
      offset = Math.abs(rr.nextLong() / 2);
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 0);
      assertEquals(1, bit);
      bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      assertEquals(0, bit);
      if (i % 10000 == 0) {
        log.debug(i);
      }
    }
    count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(0, (int) count);
    end = System.currentTimeMillis();
    log.debug("Time for {} existing SetBit/GetBit/CountBits ={}ms", nBits, end - start);
  }

  @Test
  public void testPerformance() {

    long offset;
    long max = Long.MIN_VALUE;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("");
    long start = System.currentTimeMillis();
    long expected = nBits;
    for (int i = 0; i < nBits; i++) {
      offset = Math.abs(r.nextLong() / 2);

      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (bit == 1) {
        expected--;
      }
      if (offset > max) {
        max = offset;
      }
      if (i % 100000 == 0) {
        log.debug("SetBit {}", i);
      }
    }
    long end = System.currentTimeMillis();
    long memory = UnsafeAccess.getAllocatedMemory();
    log.debug("Total RAM    ={}", memory);
    log.debug("Total loaded ={}", expected);

    long count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, count);

    log.debug("Time for {} new SetBit={}ms", nBits, end - start);
    log.debug("Compression ratio={}", (double) max / (8 * memory));
    BigSortedMap.printGlobalMemoryAllocationStats();

    r.setSeed(seed);
    start = System.currentTimeMillis();
    for (int i = 0; i < nBits; i++) {
      offset = Math.abs(r.nextLong() / 2);
      int bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      assertEquals(1, bit);
      if (i % 100000 == 0) {
        log.debug("GetBit {}", i);
      }
    }
    end = System.currentTimeMillis();
    log.debug("Time for {} GetBit={}ms", nBits, end - start);

    r.setSeed(seed);

    start = System.currentTimeMillis();
    for (int i = 0; i < nBits; i++) {
      offset = Math.abs(r.nextLong() / 2);
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 0);
      assertEquals(1, bit);
      if (i % 100000 == 0) {
        log.debug("SetBit erase {}", i);
      }
    }
    end = System.currentTimeMillis();
    log.debug("Time for {} SetBit erase={}ms", nBits, end - start);
    assertEquals(0, (int) map.countRecords());
  }

  @Test
  public void testDeleteExists() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("");
    long expected = nBits / 10;
    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextLong() / 2);

      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (bit == 1) {
        expected--;
      }
      if (i % 100000 == 0) {
        log.debug("DeleteEixts {}", i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ log.debug("Total RAM    ={}", memory);
    /*DEBUG*/ log.debug("Total loaded ={}", expected);

    long count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, count);

    assertTrue(SparseBitmaps.EXISTS(map, key.address, key.length));
    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) map.countRecords());
  }

  @Test
  public void testBitCounts() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);
    TreeSet<Integer> bits = new TreeSet<>();
    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextInt());
      bits.add((int) offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (i % 100000 == 0) {
        log.debug("BitCounts {}", i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/
    log.debug("Total RAM    ={}", memory);
    /*DEBUG*/
    log.debug("Total loaded ={}", bits.size());
    int size = bits.size();
    int strlen = bits.last() / Utils.BITS_PER_BYTE + 1;
    log.debug("Edge cases ");
    // Test 1: no start, end limits
    long count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(size, (int) count);

    assertEquals(strlen, (int) SparseBitmaps.SSTRLEN(map, key.address, key.length));

    // Test 2: no end limit
    count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Long.MIN_VALUE, Commons.NULL_LONG);
    assertEquals(size, (int) count);

    // Test 3: no start limit
    count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Integer.MAX_VALUE);
    assertEquals(size, (int) count);

    // Test 4: end < start (both positive)
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, 100, 99);
    assertEquals(0, (int) count);

    // Test 5: end < start (both negative)
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, -99, -100);
    assertEquals(0, (int) count);

    // Test 6: start and end cover all
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, -2 * strlen, 2 * strlen);
    assertEquals(size, (int) count);

    // Test 7: negatives
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, -strlen, -1);
    assertEquals(size, (int) count);

    log.debug("Edge cases start=end");

    // Test 8: start = end
    for (int i = 0; i < 100000; i++) {
      int index = r.nextInt(strlen);
      int expected = expectedNumber(bits, index, index);
      count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, index, index);
      assertEquals(expected, (int) count);

      if (i % 1000 == 0) {
        log.debug("start=end {}", i);
      }
    }
    log.debug("Random tests");
    r.setSeed(seed);
    for (int i = 0; i < nBits / 10; i++) {
      int x1 = r.nextInt(2 * strlen);
      x1 -= strlen;
      int x2 = r.nextInt(2 * strlen);
      x2 -= strlen;
      int start, end;
      if (x1 > x2) {
        end = x1;
        start = x2;
      } else {
        end = x2;
        start = x1;
      }
      int expected = expectedNumber(bits, start, end);
      long total = SparseBitmaps.SBITCOUNT(map, key.address, key.length, start, end);
      assertEquals(expected, (int) total);
      if (i % 1000 == 0) {
        log.debug("random bc {}", i);
      }
    }

    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) map.countRecords());
  }

  @Test
  public void testBitPositions() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);
    TreeSet<Integer> bits = new TreeSet<>();
    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextInt());
      bits.add((int) offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (i % 100000 == 0) {
        log.debug("BitPos {}", i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/
    log.debug("Total RAM    ={}", memory);
    /*DEBUG*/
    log.debug("Total loaded ={}", bits.size());
    int strlen = bits.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen, (int) SparseBitmaps.SSTRLEN(map, key.address, key.length));

    log.debug("Edge cases ");
    // Test 1: no start, end limits bit = 1
    // bit == 1
    int bit = 1;
    long pos =
        SparseBitmaps.SBITPOS(
            map, key.address, key.length, bit, Commons.NULL_LONG, Commons.NULL_LONG);
    int expected = expectedPositionSet(bits, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);

    // bit == 0
    bit = 0;
    expected = expectedPositionUnSet(bits, Commons.NULL_LONG, Commons.NULL_LONG);
    pos =
        SparseBitmaps.SBITPOS(
            map, key.address, key.length, bit, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);

    // Test 2: no end limit

    bit = 1;
    pos =
        SparseBitmaps.SBITPOS(
            map, key.address, key.length, bit, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    expected = expectedPositionSet(bits, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);

    bit = 0;
    pos =
        SparseBitmaps.SBITPOS(
            map, key.address, key.length, bit, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    expected = expectedPositionUnSet(bits, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);

    // Test 3: no start limit
    bit = 1;
    pos =
        SparseBitmaps.SBITPOS(
            map, key.address, key.length, bit, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    expected = expectedPositionSet(bits, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    assertEquals(expected, (int) pos);

    bit = 0;
    pos =
        SparseBitmaps.SBITPOS(
            map, key.address, key.length, bit, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    expected = expectedPositionUnSet(bits, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    assertEquals(expected, (int) pos);

    // Test 4: end < start (both positive)
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, 100, 99);
    assertEquals(-1, (int) pos);

    bit = 0;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, 100, 99);
    assertEquals(-1, (int) pos);

    // Test 5: end < start (both negative)
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -99, -100);
    assertEquals(-1, (int) pos);

    // Test 6: start and end cover all
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -2 * strlen, 2 * strlen);
    assertEquals(bits.first(), (int) pos, 0.0);

    bit = 0;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -2 * strlen, 2 * strlen);
    expected = expectedPositionUnSet(bits, -2 * strlen, 2 * strlen);
    assertEquals(expected, (int) pos);

    // Test 7: negatives
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -strlen, -1);
    assertEquals(bits.first(), (int) pos, 0.0);

    log.debug("Edge cases start=end");

    // Test 8: start = end
    for (int i = 0; i < 100000; i++) {
      int index = r.nextInt(strlen);
      expected = expectedPositionSet(bits, index, index);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 1, index, index);
      assertEquals(expected, (int) pos);

      expected = expectedPositionUnSet(bits, index, index);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 0, index, index);
      assertEquals(expected, (int) pos);
      if (i % 10000 == 0) {
        log.debug("start=end {}", i);
      }
    }

    log.debug("Random tests");
    r.setSeed(seed);
    for (int i = 0; i < nBits / 10; i++) {
      int x1 = r.nextInt(2 * strlen);
      x1 -= strlen;
      int x2 = r.nextInt(2 * strlen);
      x2 -= strlen;
      int start, end;
      if (x1 > x2) {
        end = x1;
        start = x2;
      } else {
        end = x2;
        start = x1;
      }
      expected = expectedPositionSet(bits, start, end);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 1, start, end);
      assertEquals(expected, (int) pos);
      expected = expectedPositionUnSet(bits, start, end);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 0, start, end);
      assertEquals(expected, (int) pos);
      if (i % 10000 == 0) {
        log.debug("random bc {}", i);
      }
    }

    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) map.countRecords());
  }

  @Test
  public void testBitcountPerformance() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("");
    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextInt());
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (i % 100000 == 0) {
        log.debug("BitCounts {}", i);
      }
    }
    long strlen = SparseBitmaps.SSTRLEN(map, key.address, key.length);

    log.debug("Random tests");
    r.setSeed(seed);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      int x1 = r.nextInt((int) strlen);
      int x2 = r.nextInt((int) strlen);
      int start, end;
      if (x1 > x2) {
        end = x1;
        start = x2;
      } else {
        end = x2;
        start = x1;
      }
      // TODO validate total?
      long total = SparseBitmaps.SBITCOUNT(map, key.address, key.length, start, end);
      if (i % 1000 == 0) {
        log.debug("random bc {}", i);
      }
    }
    long endTime = System.currentTimeMillis();
    log.debug(
        "SBITCOUNT for bitmap={} long ={} RPS",
        strlen,
        (double) 1000 * 1000 / (endTime - startTime));

    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) map.countRecords());
  }

  /**
   * Get expected number of bits
   *
   * @param set set
   * @param start from offset (inclusive)
   * @param end to offset (inclusive)
   * @return number of bits set
   */
  private static int expectedNumber(NavigableSet<Integer> set, long start, long end) {

    int strlen = set.last() / Utils.BITS_PER_BYTE + 1;
    if (start == Commons.NULL_LONG) {
      start = 0;
    }

    if (end == Commons.NULL_LONG || end >= strlen) {
      end = strlen - 1;
    }

    if (start >= strlen) {
      return 0;
    }

    if (start < 0) {
      start = strlen + start;
    }

    if (start < 0) {
      start = 0;
    }

    if (end != Commons.NULL_LONG && end < 0) {
      end = strlen + end;
    }

    if (end < start || end < 0) {
      return 0;
    }

    Iterator<Integer> it = set.iterator();
    int count = 0;
    while (it.hasNext()) {
      Integer v = it.next();
      if (between(start, end, v)) {
        count++;
      }
    }
    return count;
  }

  private static boolean between(long start, long end, int value) {
    int off = value / Utils.BITS_PER_BYTE;
    return off >= start && off <= end;
  }

  /**
   * Get expected position for set bit
   *
   * @param set set
   * @param start from offset (inclusive)
   * @param end to offset (inclusive)
   * @return forts set bit position
   */
  private static int expectedPositionUnSet(NavigableSet<Integer> set, long start, long end) {

    if (set.isEmpty()) {
      return 0;
    }

    int strlen = set.last() / Utils.BITS_PER_BYTE + 1;
    if (start == Commons.NULL_LONG) {
      start = 0;
    }

    if (end == Commons.NULL_LONG || end >= strlen) {
      end = strlen - 1;
    }

    if (start >= strlen) {
      return -1;
    }

    if (start < 0) {
      start = strlen + start;
    }

    if (start < 0) {
      start = 0;
    }

    if (end != Commons.NULL_LONG && end < 0) {
      end = strlen + end;
    }

    if (end < start || end < 0) {
      return -1;
    }

    long startOff = start * Utils.BITS_PER_BYTE;
    long endPos = (end + 1) * Utils.BITS_PER_BYTE;
    for (long off = startOff; off < endPos; off++) {
      if (set.contains((int) off)) {
        continue;
      }
      return (int) off;
    }

    return -1;
  }

  private static int expectedPositionSet(NavigableSet<Integer> set, long start, long end) {

    if (set.isEmpty()) {
      return -1;
    }
    int strlen = set.last() / Utils.BITS_PER_BYTE + 1;
    if (start == Commons.NULL_LONG) {
      start = 0;
    }

    if (end == Commons.NULL_LONG || end >= strlen) {
      end = strlen - 1;
    }

    if (start >= strlen) {
      return -1;
    }

    if (start < 0) {
      start = strlen + start;
    }

    if (start < 0) {
      start = 0;
    }

    if (end != Commons.NULL_LONG && end < 0) {
      end = strlen + end;
    }

    if (end < start || end < 0) {
      return -1;
    }

    Integer v = set.ceiling((int) (start * Utils.BITS_PER_BYTE));
    if (v != null && v < ((end + 1) * Utils.BITS_PER_BYTE)) {
      return v;
    }
    return -1;
  }

  @Test
  public void testBitGetRange() {

    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);
    TreeSet<Integer> bits = new TreeSet<>();

    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextInt() / 10);
      bits.add((int) offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (i % 100000 == 0) {
        log.debug("BitGetRange {}", i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    log.debug("Total RAM    ={}", memory);
    log.debug("Total loaded ={}", bits.size());

    int strlen = bits.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen, (int) SparseBitmaps.SSTRLEN(map, key.address, key.length));
    // Check bit counts
    assertEquals(
        bits.size(),
        (int)
            SparseBitmaps.SBITCOUNT(
                map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG));
    long buffer = UnsafeAccess.malloc(strlen); // buffer size to fit all bitmap
    int bufferSize = strlen;

    log.debug("Edge cases ");
    // Test 1: no start, end limits bit = 1
    int expected = expectedNumber(bits, Commons.NULL_LONG, Commons.NULL_LONG);
    long range =
        SparseBitmaps.SGETRANGE(
            map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG, buffer, bufferSize);
    assertEquals(strlen, (int) range);
    long count = Utils.bitcount(buffer, (int) range);
    assertEquals(expected, (int) count);

    // Clear buffer
    UnsafeAccess.setMemory(buffer, bufferSize, (byte) 0);

    // Test 2: no end limit

    expected = expectedNumber(bits, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    range =
        SparseBitmaps.SGETRANGE(
            map,
            key.address,
            key.length,
            Long.MIN_VALUE + 1,
            Commons.NULL_LONG,
            buffer,
            bufferSize);
    assertEquals(strlen, (int) range);
    count = Utils.bitcount(buffer, (int) range);
    assertEquals(expected, (int) count);
    // Clear buffer
    UnsafeAccess.setMemory(buffer, bufferSize, (byte) 0);

    // Test 3: no start limit
    expected = expectedNumber(bits, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    range =
        SparseBitmaps.SGETRANGE(
            map,
            key.address,
            key.length,
            Commons.NULL_LONG,
            Long.MAX_VALUE >>> 4,
            buffer,
            bufferSize);
    assertEquals(strlen, (int) range);
    count = Utils.bitcount(buffer, (int) range);
    assertEquals(expected, (int) count);
    // Clear buffer
    UnsafeAccess.setMemory(buffer, bufferSize, (byte) 0);

    // Test 4: end < start (both positive)

    // TODO validate expected?
    expected = expectedNumber(bits, 100, 99);
    range = SparseBitmaps.SGETRANGE(map, key.address, key.length, 100, 99, buffer, bufferSize);
    assertEquals(0, (int) range);

    // Test 5: end < start (both negative)
    // TODO validate expected?
    expected = expectedNumber(bits, -99, -100);
    range = SparseBitmaps.SGETRANGE(map, key.address, key.length, -99, -100, buffer, bufferSize);
    assertEquals(0, (int) range);

    // Test 6: start and end cover all
    expected = expectedNumber(bits, -2 * strlen, 2 * strlen);
    range =
        SparseBitmaps.SGETRANGE(
            map, key.address, key.length, -2 * strlen, 2 * strlen, buffer, bufferSize);
    assertEquals(strlen, (int) range);
    count = Utils.bitcount(buffer, (int) range);
    assertEquals(expected, (int) count);
    // Clear buffer
    UnsafeAccess.setMemory(buffer, bufferSize, (byte) 0);

    // Test 7: negatives
    expected = expectedNumber(bits, -strlen, -1);
    range = SparseBitmaps.SGETRANGE(map, key.address, key.length, -strlen, -1, buffer, bufferSize);
    assertEquals(strlen, (int) range);
    count = Utils.bitcount(buffer, (int) range);
    assertEquals(expected, (int) count);
    // Clear buffer
    UnsafeAccess.setMemory(buffer, bufferSize, (byte) 0);

    log.debug("Edge cases start=end");

    // Test 8: start = end
    for (int i = 0; i < 1000; i++) {
      int index = r.nextInt(strlen);
      expected = expectedNumber(bits, index, index);
      range =
          SparseBitmaps.SGETRANGE(map, key.address, key.length, index, index, buffer, bufferSize);
      assertEquals(1, (int) range);
      count = Utils.bitcount(buffer, (int) range);
      assertEquals(expected, (int) count);
      // Clear first byte of a buffer
      UnsafeAccess.putByte(buffer, (byte) 0);

      if (i % 10000 == 0) {
        log.debug("start=end {}", i);
      }
    }

    // Test 9: random tests
    log.debug("Random tests");
    r.setSeed(seed);
    for (int i = 0; i < 1000; i++) {
      int x1 = r.nextInt(2 * strlen);
      x1 -= strlen;
      int x2 = r.nextInt(2 * strlen);
      x2 -= strlen;
      int start, end;
      if (x1 > x2) {
        end = x1;
        start = x2;
      } else {
        end = x2;
        start = x1;
      }
      expected = expectedNumber(bits, start, end);
      range = SparseBitmaps.SGETRANGE(map, key.address, key.length, start, end, buffer, bufferSize);
      count = Utils.bitcount(buffer, (int) range);
      assertEquals(expected, (int) count);

      if (i % 100 == 0) {
        log.debug("random bc {}", i);
      }
    }

    // Test 10: batch scanning

    log.debug("Batch reading");

    int batchSize = strlen / 100;
    int off = 0;
    long bitCount =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    long bcount = 0;

    while (off < strlen) {
      long rr =
          SparseBitmaps.SGETRANGE(
              map, key.address, key.length, off, off + batchSize - 1, buffer, bufferSize);
      bcount += Utils.bitcount(buffer, (int) rr);
      off += batchSize;
    }

    assertEquals(bitCount, bcount);
    UnsafeAccess.free(buffer);

    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) map.countRecords());
  }

  @Test
  public void testSparseLength() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    long max = -Long.MAX_VALUE;
    long start = System.currentTimeMillis();
    long totalCount = 0;
    for (int i = 0; i < nBits; i++) {
      offset = Math.abs(r.nextLong() / 2);
      int old = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      if (old == 1) {
        continue;
      }
      if (offset > max) {
        max = offset;
      }
      int v = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      assertEquals(0, v);
      totalCount++;
      long len = SparseBitmaps.SSTRLEN(map, key.address, key.length);
      long expectedlength = (max / Utils.BITS_PER_BYTE) + 1;
      assertEquals(expectedlength, len);
      if (i % 10000 == 0 && i > 0) {
        log.debug(i);
      }
    }

    long end = System.currentTimeMillis();

    log.debug("\nTotal RAM={}\n", UnsafeAccess.getAllocatedMemory());
    BigSortedMap.printGlobalMemoryAllocationStats();

    long count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(totalCount, count);
    log.debug("Time for {} SetBit/BitCount/StrLength ={}ms", nBits, end - start);
  }

  @Test
  public void testBitSetRange() {

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);
    TreeSet<Integer> bits = new TreeSet<>();
    log.debug("Loading first sparse ");
    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextInt() / 10);
      bits.add((int) offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if ((i + 1) % 100000 == 0) {
        log.debug("BitSetRange {}", i + 1);
      }
    }

    log.debug("Loaded key1 {} bits", bits.size());

    key2 = getKey();
    log.debug("Loading second sparse ");
    TreeSet<Integer> bits2 = new TreeSet<>();

    for (int i = 0; i < nBits / 10; i++) {
      offset = Math.abs(r.nextInt() / 10);
      bits2.add((int) offset);
      SparseBitmaps.SSETBIT(map, key2.address, key2.length, offset, 1);
      if ((i + 1) % 100000 == 0) {
        log.debug("BitSetRange2 {}", i + 1);
      }
    }
    log.debug("Loaded key2 {} bits", bits2.size());

    long strlen1 = bits.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen1, SparseBitmaps.SSTRLEN(map, key.address, key.length));
    long strlen2 = bits2.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen2, SparseBitmaps.SSTRLEN(map, key2.address, key2.length));
    assertEquals(
        bits.size(),
        (int)
            SparseBitmaps.SBITCOUNT(
                map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG));
    assertEquals(
        bits2.size(),
        (int)
            SparseBitmaps.SBITCOUNT(
                map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG));

    // Test 1: small overwrites <= BYTES_PER_CHUNK
    int bufferSize = (int) Math.max(strlen1, strlen2);
    long buffer = UnsafeAccess.malloc(bufferSize);
    long buffer2 = UnsafeAccess.malloc(bufferSize);

    int strlen = (int) Math.min(strlen1, strlen2);
    // Original bit count for key2 sparse
    long bc =
        SparseBitmaps.SBITCOUNT(
            map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);

    log.debug("Running small overwrites sub-test");
    for (int i = 0; i < 10000; i++) {
      int len = r.nextInt(SparseBitmaps.BYTES_PER_CHUNK) + 1;
      int off = r.nextInt(strlen - len);
      long rbc1 = SparseBitmaps.SBITCOUNT(map, key.address, key.length, off, off + len - 1);
      long rbc2 = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, off, off + len - 1);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer, len, (byte) 0);

      int rangeSize =
          (int)
              SparseBitmaps.SGETRANGE(
                  map, key.address, key.length, off, off + len - 1, buffer, bufferSize);
      long size = SparseBitmaps.SSETRANGE(map, key2.address, key2.length, off, buffer, rangeSize);
      assertEquals(strlen2, size);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer2, len, (byte) 0);
      SparseBitmaps.SGETRANGE(
          map, key.address, key.length, off, off + len - 1, buffer2, bufferSize);

      assertEquals(0, Utils.compareTo(buffer, len, buffer2, len));

      long newbc =
          SparseBitmaps.SBITCOUNT(
              map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
      assertEquals(bc + rbc1 - rbc2, newbc);
      bc = newbc;
      if ((i + 1) % 1000 == 0) {
        log.debug("small {}", i + 1);
      }
    }
    // Test 2: running larger overwrites
    // bc contains valid bit number
    log.debug("Running larger overwrites sub-test");

    for (int i = 0; i < 10000; i++) {
      int len = r.nextInt(SparseBitmaps.BYTES_PER_CHUNK * 100) + 1;
      int off = r.nextInt(strlen - len);
      long rbc1 = SparseBitmaps.SBITCOUNT(map, key.address, key.length, off, off + len - 1);
      long rbc2 = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, off, off + len - 1);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer, len, (byte) 0);
      int rangeSize =
          (int)
              SparseBitmaps.SGETRANGE(
                  map, key.address, key.length, off, off + len - 1, buffer, bufferSize);
      long size = SparseBitmaps.SSETRANGE(map, key2.address, key2.length, off, buffer, rangeSize);
      assertEquals(strlen2, size);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer2, len, (byte) 0);
      SparseBitmaps.SGETRANGE(
          map, key.address, key.length, off, off + len - 1, buffer2, bufferSize);

      assertEquals(0, Utils.compareTo(buffer, len, buffer2, len));
      long newbc =
          SparseBitmaps.SBITCOUNT(
              map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
      assertEquals(bc + rbc1 - rbc2, newbc);
      bc = newbc;
      if ((i + 1) % 1000 == 0) {
        log.debug("large {}", i + 1);
      }
    }

    // Test 3: running larger overwrites out of key2 sparse range
    // bc contains valid bit number
    log.debug("Running larger overwrites out of range (but intersects) sub-test");

    for (int i = 0; i < 10000; i++) {
      int len = r.nextInt(SparseBitmaps.BYTES_PER_CHUNK * 100) + 1;
      int off = r.nextInt(strlen - len);
      long off2 = Math.abs(r.nextLong()) % strlen2;
      long before = SparseBitmaps.SSTRLEN(map, key2.address, key2.length);
      long rbc1 = SparseBitmaps.SBITCOUNT(map, key.address, key.length, off, off + len - 1);
      long rbc2 = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, off2, off2 + len - 1);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer, len, (byte) 0);

      int rangeSize =
          (int)
              SparseBitmaps.SGETRANGE(
                  map, key.address, key.length, off, off + len - 1, buffer, bufferSize);
      assertEquals(len, rangeSize);
      long size = SparseBitmaps.SSETRANGE(map, key2.address, key2.length, off2, buffer, rangeSize);
      // Clear first len bytes of buffer2
      UnsafeAccess.setMemory(buffer2, len, (byte) 0);

      long rangeSize2 =
          SparseBitmaps.SGETRANGE(
              map, key2.address, key2.length, off2, off2 + rangeSize - 1, buffer2, bufferSize);
      if (rangeSize2 < rangeSize) {
        // Requested range can be smaller than original b/c of a trailing zeros
        // Clear last rangeSize - size2 bytes in buffer2
        UnsafeAccess.setMemory(buffer2 + rangeSize2, rangeSize - rangeSize2, (byte) 0);
        assertEquals(size, off2 + rangeSize2);
      } else {
        assertEquals(rangeSize, (int) rangeSize2);
      }
      // Compare two ranges read after write
      int res = Utils.compareTo(buffer, rangeSize, buffer2, rangeSize);
      if (res != 0) {
        log.error(i);
        log.error("strlen1={}", strlen1);
        log.error("strlen2={}", strlen2);
        log.error("    off={}", off);
        log.error("   off2={}", off2);
        log.error("    len={}", len);
        log.error(" before={}", before);
        log.error("  after={}", size);
      }
      assertEquals(0, res);
      long newbc =
          SparseBitmaps.SBITCOUNT(
              map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
      if (newbc != bc + rbc1 - rbc2) {
        log.error(i);
      }
      assertEquals(bc + rbc1 - rbc2, newbc);
      bc = newbc;
      strlen2 = size;
      if ((i + 1) % 1000 == 0) {
        log.debug("large out {}", i + 1);
      }
    }

    UnsafeAccess.free(buffer);
    UnsafeAccess.free(buffer2);

    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    SparseBitmaps.DELETE(map, key2.address, key2.length);
    assertFalse(SparseBitmaps.EXISTS(map, key2.address, key2.length));
    assertEquals(0, (int) map.countRecords());
  }
}
