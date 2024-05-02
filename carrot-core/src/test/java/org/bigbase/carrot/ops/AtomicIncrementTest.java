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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapScanner;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

/**
 * This test load data (key - long value) and in parallel increments keys - multithreaded. At the
 * end it scans all the keys and calculated total value of all keys, compares it with expected
 * number of increments.
 */
public class AtomicIncrementTest {

  private static final Logger log = LogManager.getLogger(AtomicIncrementTest.class);

  static BigSortedMap map;
  static AtomicLong totalLoaded = new AtomicLong();
  static AtomicLong totalIncrements = new AtomicLong();
  // FIXME: no MT support yet
  static int totalThreads = 1;
  static List<Key> keys = Collections.synchronizedList(new ArrayList<Key>());

  static class IncrementRunner extends Thread {

    public IncrementRunner(String name) {
      super(name);
    }

    public void run() {

      int keySize = 16;
      byte[] key = new byte[keySize];
      long LONG_ZERO = UnsafeAccess.mallocZeroed(Utils.SIZEOF_LONG);
      Random r = new Random();

      long incrTime = 0;
      long putTime = 0;
      while (true) {  
        double d = r.nextDouble();
        if (d < 0.5 && totalLoaded.get() > 1000) {
          // Run increment
          int n = r.nextInt((int) keys.size());
          Key k = keys.get(n);
          try {
            long t1 = System.nanoTime();
            map.incrementLongOp(k.address, k.length, 1);
            long t2 = System.nanoTime();
            incrTime += t2 - t1;
          } catch (OperationFailedException e) {
            log.error("Increment failed.", e);
            break;
          }
          totalIncrements.incrementAndGet();

        } else {
          // Run put
          totalLoaded.incrementAndGet();
          Key k = nextKey(r, key);
          long t1 = System.nanoTime();
          boolean result = map.put(k.address, k.length, LONG_ZERO, Utils.SIZEOF_LONG, 0);
          long t2 = System.nanoTime();
          putTime += t2 - t1;
          if (result == false) {
            totalLoaded.decrementAndGet();
            break;
          } else {
            keys.add(k);
          }
          if ((totalLoaded.get() + 1) % 1000000 == 0) {
            log.debug(
                "{} loaded = {} PPS={} IPS={} mem={} max={}",
                getName(),
                totalLoaded,
                totalLoaded.get() * 1_000_000_000/ putTime,
                totalIncrements.get() * 1_000_000_000/ incrTime,
                BigSortedMap.getGlobalAllocatedMemory(),
                BigSortedMap.getGlobalMemoryLimit());
          }
        }
      } // end while
    } // end run()

    private Key nextKey(Random r, byte[] buf) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.allocAndCopy(buf, 0, buf.length);
      Key k = new Key(ptr, buf.length);
      return k;
    }
  } // end IncrementRunner

  @Test
  public void testIncrement() throws IOException {
    //BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    for (int k = 1; k <= 1; k++) {
      log.debug("Increment test run #{}", k);

      BigSortedMap.setMaxBlockSize(4096);
      map = new BigSortedMap(1000000000L);
      totalLoaded.set(0);
      totalIncrements.set(0);
      try {
        long start = System.currentTimeMillis();
        IncrementRunner[] runners = new IncrementRunner[totalThreads];
        for (int i = 0; i < totalThreads; i++) {
          runners[i] = new IncrementRunner("Increment Runner#" + i);
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
          long addr = scanner.valueAddress();
          total += UnsafeAccess.toLong(addr);
          scanner.next();
        }
        log.debug("totalLoaded={} actual={}", totalLoaded, count);

        assertEquals(totalIncrements.get(), total);
        // CHECK THIS
        assertEquals(keys.size(), (int) count);
        log.debug(
            "Time to load= {} and to increment ={} is {}ms",
            totalLoaded,
            totalIncrements,
            end - start);
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
}
