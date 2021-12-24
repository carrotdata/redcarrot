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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Value;
import org.junit.Ignore;
import org.junit.Test;

public class ZSetsMultithreadedTest {

  private static final Logger log = LogManager.getLogger(ZSetsMultithreadedTest.class);

  BigSortedMap map;
  int valueSize = 16;
  int keySize = 16;
  int setSize = 1000;
  int keysNumber = 50; // per thread
  int numThreads = 1;
  List<Value> values;
  List<Double> scores;
  long setupTime;

  static {
    UnsafeAccess.setMallocDebugEnabled(true);
    //    UnsafeAccess.setMallocDebugStackTraceEnabled(true);
    //    UnsafeAccess.setStackTraceRecordingFilter((x) -> x >= 2000);
    //    UnsafeAccess.setStackTraceRecordingLimit(10000);
  }

  private List<Value> getValues() {
    byte[] buffer = new byte[valueSize / 2];
    Random r = new Random();
    values = new ArrayList<Value>();
    for (int i = 0; i < setSize; i++) {
      long ptr = UnsafeAccess.malloc(valueSize);
      int size = valueSize;
      r.nextBytes(buffer);
      UnsafeAccess.copy(buffer, 0, ptr, valueSize / 2);
      UnsafeAccess.copy(buffer, 0, ptr + valueSize / 2, valueSize / 2);
      values.add(new Value(ptr, size));
    }
    return values;
  }

  private List<Double> getScores() {
    Random r = new Random();
    scores = new ArrayList<Double>();
    for (int i = 0; i < setSize; i++) {
      scores.add(r.nextDouble());
    }
    return scores;
  }

  // @Before
  private void setUp() {
    setupTime = System.currentTimeMillis();
    map = new BigSortedMap(100000000000L);
    values = getValues();
    scores = getScores();
  }

  // @After
  private void tearDown() {
    map.dispose();
    values.stream().forEach(x -> UnsafeAccess.free(x.address));
  }

  // @Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {} Compression=NULL", i + 1);
      setUp();
      runTest();
      tearDown();

      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats("runAllNoCompression");
    }
  }

  @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    log.debug("");
    for (int i = 0; i < 100; i++) {
      log.debug("*************** RUN = {} Compression=LZ4", i + 1);
      setUp();
      runTest();
      tearDown();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats("runAllCompressionLZ4");
    }
  }

  @Ignore
  @Test
  public void runTest() {

    Runnable load =
        new Runnable() {

          @Override
          public void run() {
            int loaded = 0;
            // Name is string int
            String name = Thread.currentThread().getName();
            int id = Integer.parseInt(name);
            Random r = new Random(setupTime + id);
            long ptr = UnsafeAccess.malloc(keySize);
            byte[] buf = new byte[keySize];
            for (int i = 0; i < keysNumber; i++) {
              r.nextBytes(buf);
              UnsafeAccess.copy(buf, 0, ptr, keySize);
              long[] vptrs = new long[1];
              int[] vsizes = new int[1];
              double[] scs = new double[1];

              for (int j = 0; j < setSize; j++) {
                Value v = values.get(j);

                vptrs[0] = v.address;
                vsizes[0] = v.length;
                scs[0] = scores.get(j);
                int res = (int) ZSets.ZADD(map, ptr, keySize, scs, vptrs, vsizes, false);
                assertEquals(1, res);
                Double d = ZSets.ZSCORE(map, ptr, keySize, v.address, v.length);
                assertEquals(scs[0], d, 0.0);
                loaded++;
                if (loaded % 10000 == 0) {
                  log.debug("{} loaded {}", Thread.currentThread().getName(), loaded);
                }
              }
              int card = (int) ZSets.ZCARD(map, ptr, keySize);
              if (card != values.size()) {
                card = (int) ZSets.ZCARD(map, ptr, keySize);
                log.fatal("Second CARD={}", card);
                Thread.dumpStack();
                System.exit(-1);
              }
              assertEquals(values.size(), card);
            }
            UnsafeAccess.free(ptr);
          }
        };
    Runnable get =
        new Runnable() {

          @Override
          public void run() {
            int read = 0;
            // Name is string int
            String name = Thread.currentThread().getName();
            int id = Integer.parseInt(name);
            Random r = new Random(setupTime + id);
            long ptr = UnsafeAccess.malloc(keySize);
            long buffer = UnsafeAccess.malloc(valueSize);
            byte[] buf = new byte[keySize];
            for (int i = 0; i < keysNumber; i++) {
              r.nextBytes(buf);
              UnsafeAccess.copy(buf, 0, ptr, keySize);
              for (int j = 0; j < setSize; j++) {
                Value v = values.get(j);
                double expScore = scores.get(j);
                Double res = ZSets.ZSCORE(map, ptr, keySize, v.address, v.length);
                assertNotNull(res);
                assertEquals(expScore, res, 0.0);
                read++;
                if (read % 1000 == 0) {
                  log.debug("{} read {}", Thread.currentThread().getName(), read);
                }
              }
            }
            UnsafeAccess.free(ptr);
            UnsafeAccess.free(buffer);
          }
        };

    Runnable delete =
        new Runnable() {

          @Override
          public void run() {
            // Name is string int
            String name = Thread.currentThread().getName();
            int id = Integer.parseInt(name);
            Random r = new Random(setupTime + id);
            long ptr = UnsafeAccess.malloc(keySize);
            byte[] buf = new byte[keySize];

            for (int i = 0; i < keysNumber; i++) {
              r.nextBytes(buf);
              UnsafeAccess.copy(buf, 0, ptr, keySize);
              long card = (int) ZSets.ZCARD(map, ptr, keySize);
              if (card != setSize) {
                log.fatal("card:{} != setSize:{}", card, setSize);
                Thread.dumpStack();
                System.exit(-1);
              }
              assertEquals(setSize, (int) card);
              boolean res = ZSets.DELETE(map, ptr, keySize);
              assertTrue(res);
              card = ZSets.ZCARD(map, ptr, keySize);
              if (card != 0) {
                log.fatal("delete, card={}", card);
                System.exit(-1);
              }
              assertEquals(0L, card);
            }
            UnsafeAccess.free(ptr);
          }
        };

    log.debug("Loading data");
    Thread[] workers = new Thread[numThreads];

    long start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(load, Integer.toString(i));
      workers[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
    }

    long end = System.currentTimeMillis();
    BigSortedMap.printGlobalMemoryAllocationStats();

    log.debug(
        "Loading {} elements os done in {}ms", numThreads * keysNumber * setSize, end - start);
    log.debug("Reading data");
    start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(get, Integer.toString(i));
      workers[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
    }

    end = System.currentTimeMillis();

    log.debug(
        "Reading {} elements os done in {}ms", numThreads * keysNumber * setSize, end - start);
    log.debug("Deleting  data");
    start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(delete, Integer.toString(i));
      workers[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
    }
    end = System.currentTimeMillis();
    log.debug("Deleting of {} sets in {}ms", numThreads * keysNumber, end - start);
    assertEquals(0L, map.countRecords());
  }
}
