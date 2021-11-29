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
package org.bigbase.carrot.redis.zsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.CarrotCoreBase;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.bigbase.carrot.util.Value;
import org.bigbase.carrot.util.ValueScore;
import org.junit.Test;

public class ZSetsTest extends CarrotCoreBase{

  private static final Logger log = LogManager.getLogger(ZSetsTest.class);

  static BigSortedMap map;
  static Key key;
  static long buffer;
  static int bufferSize = 64;
  static int fieldSize = 16;
  static long n = 10000;
  static List<Value> fields;
  static List<Double> scores;
  static int maxScore = 100000;

  public ZSetsTest(Object c) throws IOException {
    super(c);
    tearDown();
    setUp();
  } 
  
  private static List<Value> getFields(long n) {
    List<Value> keys = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("KEYS SEED={}", seed);
    byte[] buf = new byte[fieldSize / 2];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(fieldSize);
      // Make values compressible
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      UnsafeAccess.copy(buf, 0, ptr + buf.length, buf.length);
      keys.add(new Value(ptr, fieldSize));
    }
    return keys;
  }

  private static List<Double> getScores(long n) {
    List<Double> scores = new ArrayList<>();
    Random r = new Random(1);
    for (int i = 0; i < n; i++) {
      scores.add((double) r.nextInt(maxScore));
    }
    return scores;
  }

  private static Key getKey() {
    long ptr = UnsafeAccess.malloc(fieldSize);
    byte[] buf = new byte[fieldSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("SEED={}", seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, fieldSize);
    return key = new Key(ptr, fieldSize);
  }

  private static void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    fields = getFields(n);
    scores = getScores(n);
    Utils.sortKeys(fields);
    for (int i = 1; i < n; i++) {
      Key prev = fields.get(i - 1);
      Key cur = fields.get(i);
      int res = Utils.compareTo(prev.address, prev.length, cur.address, cur.length);
      if (res == 0) {
        log.debug("Found duplicate");
        fail();
      }
    }
  }


  @Test
  public void testAddGetScoreMulti() {
    log.debug("Test ZSet Add Get Score Multi {}", getParameters());
    Key key = getKey();
    long[] elemPtrs = new long[(int) n];
    int[] elemSizes = new int[(int) n];
    double[] scores = new double[(int) n];
    int len = scores.length;
    List<Value> fields = ZSetsTest.fields;
    List<Double> scl = ZSetsTest.scores;
    for (int i = 0; i < len; i++) {
      elemPtrs[i] = fields.get(i).address;
      elemSizes[i] = fields.get(i).length;
      scores[i] = scl.get(i);
    }

    long start = System.nanoTime();
    long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
    long end = System.nanoTime();
    log.debug("call time={}micros", (end - start) / 1000);
    assertEquals((int) n, (int) num);
    assertEquals((int) n, (int) ZSets.ZCARD(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      Double res = ZSets.ZSCORE(map, key.address, key.length, elemPtrs[i], elemSizes[i]);
      assertNotNull(res);
      assertEquals(scores[i], res, 0.0);
    }

    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));

  }

  @Test
  public void testAddGetScoreMultiOpt() {
    log.debug("Test ZSet Add Get Score Multi (Optimized version) {}", getParameters());
    Key key = getKey();

    List<Value> fields = ZSetsTest.fields;
    List<Double> scl = ZSetsTest.scores;
    List<ValueScore> list = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Value v = fields.get(i);
      double score = scl.get(i);
      list.add(new ValueScore(v.address, v.length, score));
    }

    long start = System.nanoTime();
    long num = ZSets.ZADD_NEW(map, key.address, key.length, list);
    long end = System.nanoTime();
    log.debug("call time={}micros", (end - start) / 1000);
    assertEquals((int) n, (int) num);
    assertEquals((int) n, (int) ZSets.ZCARD(map, key.address, key.length));

    for (int i = 0; i < n; i++) {
      Value v = fields.get(i);
      Double res = ZSets.ZSCORE(map, key.address, key.length, v.address, v.length);
      assertEquals(scl.get(i), res);
    }
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));
  }

  @Test
  public void testAddGetScore() {
    log.debug("Test ZSet Add Get Score {}", getParameters());
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();

    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      scores[0] = ZSetsTest.scores.get(i);
      long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int) num);
      if ((i + 1) % 100000 == 0) {
        log.debug(i + 1);
      }
    }
    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n
            - (2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3),
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, ZSets.ZCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      Double res =
          ZSets.ZSCORE(map, key.address, key.length, fields.get(i).address, fields.get(i).length);
      assertEquals(ZSetsTest.scores.get(i), res);
      if ((i + 1) % 100000 == 0) {
        log.debug(i + 1);
      }
    }
    end = System.currentTimeMillis();
    log.debug(" Time for {} ZSCORE={}ms", n, end - start);
    BigSortedMap.printGlobalMemoryAllocationStats();
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));
  }

  @Test
  public void testAddRemove() {
    log.debug("Test ZSet Add Remove {}", getParameters());
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      scores[0] = ZSetsTest.scores.get(i);
      long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int) num);
      if ((i + 1) % 100000 == 0) {
        log.debug(i + 1);
      }
    }
    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n
            - (2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3),
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, ZSets.ZCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      long n = ZSets.ZREM(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, (int) n);
      if ((i + 1) % 100000 == 0) {
        log.debug(i + 1);
      }
    }
    end = System.currentTimeMillis();
    log.debug("Time for {} ZREM={}ms", n, end - start);
    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));
  }

  @Test
  public void testAddDeleteMulti() {
    log.debug("Test ZSet Add Delete Multi {}", getParameters());
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      scores[0] = ZSetsTest.scores.get(i);
      long num = ZSets.ZADD(map, elemPtrs[0], elemSizes[0], scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int) num);
      if ((i + 1) % 100000 == 0) {
        log.debug(i + 1);
      }
    }
    int setSize =
        DataBlock.RECORD_TOTAL_OVERHEAD
            + fieldSize /*part of a key*/
            + 6 /*4 + 1 + 1 - additional key overhead */
            + Utils.SIZEOF_DOUBLE
            + fieldSize
            + 3;

    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        setSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - setSize,
        end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      boolean res = ZSets.DELETE(map, elemPtrs[0], elemSizes[0]);
      assertTrue(res);
      if ((i + 1) % 100000 == 0) {
        log.debug(i + 1);
      }
    }
    end = System.currentTimeMillis();
    log.debug("Time for {} DELETE={}ms", n, end - start);
    assertEquals(0, (int) map.countRecords());
  }

  private static void tearDown() {
    if (map == null) {
      return;
    }
    // Dispose
    map.dispose();
    if (key != null) {
      UnsafeAccess.free(key.address);
      key = null;
    }
    for (Key k : fields) {
      UnsafeAccess.free(k.address);
    }
    UnsafeAccess.free(buffer);
    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.printGlobalMemoryAllocationStats();
  }
}
