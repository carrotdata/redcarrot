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
package com.carrotdata.redcarrot.redis.sets;

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
import com.carrotdata.redcarrot.util.Value;

import org.junit.Ignore;
import org.junit.Test;

public class SetsMultithreadedTest {

  private static final Logger log = LogManager.getLogger(SetsMultithreadedTest.class);

  BigSortedMap map;
  int valueSize = 16;
  int keySize = 16;
  int setSize = 10_000; // Max 10K pages per file
  int keysNumber = 100000; //File number per thread
  int numThreads = 2;
  List<Value> values;
  boolean[] rnd = new boolean[1_000_111];
  long setupTime;
  double percCached = 0.1;// 10% of a file is cached on average

  long valueBufPtr ;

  private List<Value> getValues() {
    valueBufPtr = UnsafeAccess.malloc(setSize * Integer.toString(setSize).length());
    
    values = new ArrayList<Value>();
    int off = 0;
    for (int i = 0; i < setSize; i++) {
      byte[] b = Integer.toString(i).getBytes();
      long ptr = UnsafeAccess.mallocZeroed(b.length);
      UnsafeAccess.copy(b, 0, ptr, b.length);
      values.add(new Value(ptr, b.length));
    }
    return values;
  }
  
  private void initRandom() {
    Random r = new Random();
    for (int i = 0; i < rnd.length; i++) {
      if (r.nextDouble() < percCached) {
        rnd[i] = true;
      } else {
        rnd[i] = false;
      }
    }
  }
  
  // @Before
  private void setUp() {
    setupTime = System.currentTimeMillis();
    map = new BigSortedMap(20_000_000_000L);
    values = getValues();
    initRandom();
  }

  // @After
  private void tearDown() {
    map.dispose();
    values.stream().forEach(x -> UnsafeAccess.free(x.address));
  }

  @Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    log.debug("");
    for (int i = 0; i < 100; i++) {
      log.debug("*************** RUN = {} Compression=NULL", i + 1);
      setUp();
      runTest();
      // Wait
      System.in.read();
      tearDown();    
    }
  }

  //@Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {}  Compression=LZ4", i + 1);
      setUp();
      runTest();
      tearDown();
    }
  }

  @Ignore
  @Test
  public void runAllCompressionZSTD() throws IOException {
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    log.debug("");
    for (int i = 0; i < 100; i++) {
      log.debug("*************** RUN = {}  Compression=ZSTD", i + 1);
      setUp();
      runTest();
      tearDown();
    }
  }

  private void runTest() throws IOException {

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
        long c = 0;
        int nn = 0;
        for (int i = 0; i < keysNumber; i++) {
          r.nextBytes(buf);
          UnsafeAccess.copy(buf, 0, ptr, keySize);
          //int off = 0;

          for (Value v : values) {
            if (!rnd[(int)(c++ % rnd.length)]) {
              continue;
            }

            int res = Sets.SADD(map, ptr, keySize, v.address, v.length);
            assertEquals(1, res);
            loaded++;
            if (loaded % 1000000 == 0) {
              log.debug("{} loaded {}", Thread.currentThread().getName(), loaded);
            }
          }
//          int card = (int) Sets.SCARD(map, ptr, keySize);
//          if (card != values.size()) {
//            card = (int) Sets.SCARD(map, ptr, keySize);
//            log.fatal("Second CARD={}", card);
//            Thread.dumpStack();
//            System.exit(-1);
//          }
//          assertEquals(values.size(), card);
        }
        UnsafeAccess.free(ptr);
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
        Random rr = new Random(setupTime + id);

        long ptr = UnsafeAccess.malloc(keySize);
        byte[] buf = new byte[keySize];
        long c = 0;
        for (int i = 0; i < keysNumber; i++) {
          r.nextBytes(buf);
          UnsafeAccess.copy(buf, 0, ptr, keySize);
          for (Value v : values) {
            if (!rnd[(int)(c++ % rnd.length)]) {
              continue;
            }
            int res = Sets.SISMEMBER(map, ptr, keySize, v.address, v.length);
            assertEquals(1, res);
            read++;
            if (read % 1000000 == 0) {
              log.debug("{} read {}", Thread.currentThread().getName(), read);
            }
          }
        }
        UnsafeAccess.free(ptr);
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
//          long card = (int) Sets.SCARD(map, ptr, keySize);
//          if (card != setSize) {
//            log.fatal("card:{} != setSize:{}", card, setSize);
//            Thread.dumpStack();
//            System.exit(-1);
//          }
//          assertEquals(setSize, (int) card);
          boolean res = Sets.DELETE(map, ptr, keySize);
          assertTrue(res);
          long card = Sets.SCARD(map, ptr, keySize);
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

    log.debug("Loading  approximately " + ((long)numThreads * keysNumber * setSize * percCached) + " elements os done in "
        + (end - start) + "ms");
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

    log.debug("Reading approximately" + ((long)numThreads * keysNumber * setSize * percCached) + " elements os done in "
        + (end - start) + "ms");
    
    System.in.read();
    
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
