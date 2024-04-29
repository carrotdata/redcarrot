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
package org.bigbase.carrot.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapScanner;
import org.bigbase.carrot.ops.Append;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.Test;

/**
 * This test load data (key - long value) and in parallel appends keys - multithreaded. At the end
 * it scans all the keys and calculated total value size of all keys, compares it with expected
 * number of appends.
 */
public class AtomicAppendTestMT {

  private static final Logger log = LogManager.getLogger(AtomicAppendTestMT.class);

  static BigSortedMap map;
  static AtomicLong totalLoaded = new AtomicLong();
  static AtomicLong totalAppends = new AtomicLong();
  static long toLoad = 2000000;
  //FIXME: no MT support yet
  static int totalThreads = 1;

  static class AppendRunner extends Thread {

    public AppendRunner(String name) {
      super(name);
    }

    public void run() {
      Append append = new Append();
      long ptr = UnsafeAccess.malloc(16);
      long value = UnsafeAccess.malloc(8);
      int keySize;
      Random r = new Random();
      byte[] LONG_ZERO = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};

      while (totalLoaded.get() < toLoad) {
        double d = r.nextDouble();
        if (d < 0.5 && totalLoaded.get() > 1000) {
          // Run append
          int n = r.nextInt((int) totalLoaded.get()) + 1;
          keySize = getKey(ptr, n);
          if (!map.exists(ptr, keySize)) {
            continue;
          }
          append.reset();
          append.setKeyAddress(ptr);
          append.setKeySize(keySize);
          append.setAppendValue(value, 8);
          boolean res = map.execute(append);
          assertTrue(res);
          totalAppends.incrementAndGet();
        } else {
          // Run put
          byte[] key = ("KEY" + (totalLoaded.incrementAndGet())).getBytes();
          byte[] vvalue = LONG_ZERO;
          boolean result = map.put(key, 0, key.length, vvalue, 0, vvalue.length, 0);
          assertTrue(result);
        }
        if (totalLoaded.get() % 1000000 == 0) {
          log.debug(getName() + " loaded = {} appends={}", totalLoaded, totalAppends);
        }
      } // end while
      UnsafeAccess.free(ptr);
      UnsafeAccess.free(value);
    } // end run()
  } // end IncrementRunner

  @Test
  public void testAppend() throws IOException {
    for (int k = 1; k <= 100; k++) {
      log.debug("Append test run #{}", k);

      BigSortedMap.setMaxBlockSize(4096);
      map = new BigSortedMap(1000000000L);
      totalLoaded.set(0);
      totalAppends.set(0);
      try {
        long start = System.currentTimeMillis();
        AppendRunner[] runners = new AppendRunner[totalThreads];
        for (int i = 0; i < totalThreads; i++) {
          runners[i] = new AppendRunner("Increment Runner#" + i);
          runners[i].start();
        }
        for (int i = 0; i < totalThreads; i++) {
          try {
            runners[i].join();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            log.error("StackTrace: ", e);
          }
        }

        long end = System.currentTimeMillis();
        BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
        long total = 0;
        long count = 0;
        while (scanner.hasNext()) {
          count++;
          int size = scanner.valueSize();
          total += size;
          scanner.next();
        }
        assertEquals((totalAppends.get() + totalLoaded.get()) * 8, total);
        assertEquals(totalLoaded.get(), count);
        map.dumpStats();
        log.debug(
            "Time to load= {} and to append ={}={}ms", totalLoaded, totalAppends, end - start);
        log.debug("Total memory={}", BigSortedMap.getGlobalAllocatedMemory());
        log.debug("Total   data={}", BigSortedMap.getGlobalBlockDataSize());
        log.debug("Total  index={}", BigSortedMap.getGlobalBlockIndexSize());
      } finally {
        if (map != null) {
          map.dispose();
          map = null;
        }
      }
    }
  }

  private static int getKey(long ptr, int n) {
    byte[] key = ("KEY" + (n)).getBytes();
    UnsafeAccess.copy(key, 0, ptr, key.length);
    return key.length;
  }
}
