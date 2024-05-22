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
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;
import com.carrotdata.redcarrot.util.Value;
import org.junit.Test;

public class HashesMultithreadedTest {

  private static final Logger log = LogManager.getLogger(HashesMultithreadedTest.class);

  BigSortedMap map;
  int valueSize = 16;
  int keySize = 16;
  int setSize = 10000;
  int keysNumber = 1000; // per thread
  // FIXME: no MT support yet
  int numThreads = 1;
  List<Value> values;
  long setupTime;

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

  private void setUp() {
    setupTime = System.currentTimeMillis();
    map = new BigSortedMap(100000000000L);
    values = getValues();
  }

  private void tearDown() {
    map.dispose();
    values.stream().forEach(x -> UnsafeAccess.free(x.address));
  }

  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {} Compression=NULL", i + 1);
      setUp();
      runTest();
      tearDown();
    }
  }

  @Test
  public void runAllCompressionLZ4() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {} Compression=LZ4", i + 1);
      setUp();
      runTest();
      tearDown();
    }
  }

  @Test
  public void runAllCompressionZSTD() {
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {} Compression=ZSTD", i + 1);
      setUp();
      runTest();
      tearDown();
    }
  }

  private void runTest() {

    Runnable load = new Runnable() {

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
          for (Value v : values) {
            int res = Hashes.HSET(map, ptr, keySize, v.address, v.length, v.address, v.length);
            assertEquals(1, res);
            loaded++;
            if (loaded % 1000000 == 0) {
              log.debug("{} loaded {}", Thread.currentThread().getName(), loaded);
            }
          }
          int card = (int) Hashes.HLEN(map, ptr, keySize);
          if (card != values.size()) {
            log.fatal("First CARD={}", card);
            card = (int) Hashes.HLEN(map, ptr, keySize);
            log.fatal("Second CARD={}", card);

            Thread.dumpStack();
            System.exit(-1);
          }
          assertEquals(values.size(), card);
        }
        UnsafeAccess.free(ptr);
      }

      @SuppressWarnings("unused")
      private void dump(int[] prev, int total, int[] prev2, int total2) {
        // total2 > total
        log.error("total={}, total2={}", total, total2);
        int i = 0;
        for (; i < total; i++) {
          log.error("{} {}", prev[i], prev2[i]);
        }
        for (; i < total2; i++) {
          log.error("** {}", prev2[i]);
        }
      }
    };
    Runnable get = new Runnable() {

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
          for (Value v : values) {
            int res = Hashes.HGET(map, ptr, keySize, v.address, v.length, buffer, valueSize);
            assertEquals(valueSize, res);
            assertEquals(0, Utils.compareTo(v.address, v.length, buffer, valueSize));
            read++;
            if (read % 1000000 == 0) {
              log.debug("{} read {}", Thread.currentThread().getName(), read);
            }
          }
        }
        UnsafeAccess.free(ptr);
        UnsafeAccess.free(buffer);
      }
    };

    Runnable delete = new Runnable() {

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
          long card = (int) Hashes.HLEN(map, ptr, keySize);
          if (card != setSize) {
            log.fatal("card:{} != setSize:{}", card, setSize);
            Thread.dumpStack();
            System.exit(-1);
          }
          assertEquals(setSize, (int) card);
          boolean res = Hashes.DELETE(map, ptr, keySize);
          assertTrue(res);
          card = Hashes.HLEN(map, ptr, keySize);
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

    log.debug("Loading {} elements os done in {}ms", (numThreads * keysNumber * setSize),
      end - start);
    
    BigSortedMap.printGlobalMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats("Memory Statistics:");
    
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

    log.debug("Reading {} elements os done in {}ms", (numThreads * keysNumber * setSize),
      end - start);
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
