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
package com.carrotdata.redcarrot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import org.junit.Test;

public class BigSortedMapPerfTestClusterMode {

  private static final Logger log = LogManager.getLogger(BigSortedMapPerfTestClusterMode.class);

  static class SingleRun implements Runnable {

    BigSortedMap map;
    long totalLoaded;
    long totalScanned = 0;

    private void setUp() {

      map = new BigSortedMap();
      totalLoaded = 1;
      long start = System.currentTimeMillis();
      while (true) {
        byte[] key = ("KEY" + (totalLoaded)).getBytes();
        byte[] value = ("VALUE" + (totalLoaded)).getBytes();
        boolean res = map.put(key, 0, key.length, value, 0, value.length, 0);
        if (!res) {
          totalLoaded--;
          break;
        }
        totalLoaded++;
        if (totalLoaded % 100000 == 0) {
          log.debug(
              "{}: Loaded {} RAM allocated={}",
              Thread.currentThread().getId(),
              totalLoaded,
              BigSortedMap.getGlobalAllocatedMemory());
        }
      }
      long end = System.currentTimeMillis();
      log.debug(
          "Load performance: Records={}, MS={}, RPS={}",
          totalLoaded,
          end - start,
          totalLoaded * 1000 / (end - start));
      log.debug("Total memory={}", BigSortedMap.getGlobalAllocatedMemory());
    }

    private void tearDown() {
      map.dispose();
    }

    public void testCountRecords() throws IOException {
      log.debug("testCountRecords");
      int n = 10;
      long start = System.currentTimeMillis();
      for (int i = 0; i < n; i++) {
        log.debug("{}: Scan Run started {}", Thread.currentThread().getId(), i);
        totalScanned += countRecords();
        log.debug("{}: Scan Run finished {}", Thread.currentThread().getId(), i);
      }
      long end = System.currentTimeMillis();

      log.debug("Scan performance: {}: {} RPS", Thread.currentThread().getId(), totalScanned * 1000 / (end - start));
      assertEquals(n * totalLoaded, totalScanned);
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

    @Override
    public void run() {
      setUp();
      try {
        testCountRecords();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
      tearDown();
    }
  }

  @Test
  public void testClusterPerformance() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    BigSortedMap.setGlobalMemoryLimit(100000000L);
    int numThreads = 2;
    Thread[] workers = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      SingleRun sr = new SingleRun();
      workers[i] = new Thread(sr);
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

    BigSortedMap.printGlobalMemoryAllocationStats();
  }
}
