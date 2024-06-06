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
package com.carrotdata.redcarrot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigSortedMapPerfTest {

  private static final Logger log = LogManager.getLogger(BigSortedMapPerfTest.class);

  static BigSortedMap map;
  static long totalLoaded;
  static long totalScanned = 0;

  @BeforeClass
  public static void setUp() {
    log.debug("Set up: block = 4096; Mem={}", 10000000);

    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    // BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));

    BigSortedMap.setMaxBlockSize(4096);

  }

  public long loadData() {
    map = new BigSortedMap(100000000L);
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
        log.debug("Loaded {} RAM alocated={}", totalLoaded,
          BigSortedMap.getGlobalAllocatedMemory());
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Time to load=" + totalLoaded + " =" + (end - start) + "ms" + " RPS="
        + (totalLoaded * 1000) / (end - start));
    log.debug("Total memory={}", BigSortedMap.getGlobalAllocatedMemory());
    return totalLoaded;
  }

  @Test
  public void testCountRecords() throws IOException {
    log.debug("testCountRecords");
    int n = 10;
    int c = 0;
    while (c++ < 1) {
      long totalTime = 0;
      totalScanned = 0;
      loadData();
      for (int i = 0; i < n; i++) {
        log.debug("Scan Run started {}", i);
        long start = System.currentTimeMillis();
        totalScanned += countRecords();
        totalTime += System.currentTimeMillis() - start;
        log.debug("Scan Run finished {}", i);
      }

      log.debug("{} RPS", totalScanned * 1000 / totalTime);
      BigSortedMap.printGlobalMemoryAllocationStats();
      assertEquals(n * totalLoaded, totalScanned);
      map.dispose();
    }
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
}
