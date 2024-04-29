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
package org.bigbase.carrot;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;

public class BigSortedMapTestMT {

  private static final Logger log = LogManager.getLogger(BigSortedMapTestMT.class);

  static int totalThreads = 6;
  static AtomicDouble putsPs = new AtomicDouble();
  static AtomicDouble comboPs = new AtomicDouble();
  static AtomicDouble scanPs = new AtomicDouble();
  static AtomicLong totalLoaded = new AtomicLong(0);
  static AtomicLong totalDeleted = new AtomicLong(0);
  static CyclicBarrier barrier = new CyclicBarrier(totalThreads);

  static class Worker extends Thread {
    BigSortedMap map;
    long totalOps = 0;
    int keySize = 16;
    int maxValueSize = 10000;
    long key;
    long value;
    byte[] keyBuf = new byte[keySize];
    int[] valueSizeDistribution =
        new int[] {10 /*p0*/, 100 /*p50*/, 220 /*p90*/, 450 /*p95*/, 3000 /*p99*/, 9500 /*p100*/};
//    double[] pp = new double[] {0, 0.5, 0.9, 0.95, 0.99, 1.0};
    Random r = new Random();

    public Worker(BigSortedMap map, String name) {
      super(name);
      this.map = map;
    }

    public void run() {
      try {
        // init value
        byte[] buf = new byte[maxValueSize];
        r.nextBytes(buf);
        value = UnsafeAccess.allocAndCopy(buf, 0, maxValueSize);
        key = UnsafeAccess.malloc(keySize);
        long start = System.currentTimeMillis();
        runPuts();
        long end = System.currentTimeMillis();
        putsPs.addAndGet((double) (totalOps * 1000) / (end - start));
        totalOps = 0;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        start = System.currentTimeMillis();
        runPutDeleteGetsScans();
        end = System.currentTimeMillis();
        comboPs.addAndGet((double) (totalOps * 1000) / (end - start));
        totalOps = 0;
      } catch (RuntimeException | IOException e) {
        log.fatal("StackTrace: ", e);
        System.exit(-1);
      } // TODO Auto-generated catch block
    }

    private void runPutDeleteGetsScans() throws IOException {
      log.debug("{} test runPutDeleteGetsScans", Thread.currentThread().getName());

      int num = (int) totalLoaded.get();
      long start = System.currentTimeMillis();
      Random r = new Random();
      for (int i = 0; i < num; i++) {
        double d = r.nextDouble();
        boolean result;
        if (BigSortedMap.getGlobalAllocatedMemory() < 0.99 * BigSortedMap.getGlobalMemoryLimit()) {
          if (d < 0.30) {
            result = put();
            if (!result) {
              log.fatal("BigSortedMap allocation < 0.30");
              Thread.dumpStack();
              System.exit(-1);
            }
            assertTrue(result);

          } else if (d < 0.5) {
            result = overwrite();
            assertTrue(result);
          } else if (d < 0.8) {
            result = delete();
            if (!result) {
              log.fatal("BigSortedMap allocation < 0.8");
              Thread.dumpStack();
              System.exit(-1);
            }
            assertTrue(result);

          } else if (d < 0.9) {
            // 0.9 is for safety, not all up to totalLoaded
            // can be really loaded (last ones may be in flight)
            int n = r.nextInt((int) (totalLoaded.get() * 0.9));
            long size = get(n);
            if (size < 0 && n > totalDeleted.get()) {
              fail("Not found " + n + " deleted=" + totalDeleted.get());
            }
            if (size > 0) {
              assertTrue(
                  Thread.currentThread().getName()
                      + " ERROR  num="
                      + i
                      + " totalLoaded="
                      + totalLoaded.get()
                      + " size="
                      + size,
                  size >= valueSizeDistribution[0]
                      && size <= valueSizeDistribution[valueSizeDistribution.length - 1]);
            }
          } else {
            int n = r.nextInt((int) (totalLoaded.get() * 0.9));
            scan(n);
          }
        } else {
          // Do not do puts
          if (d < 0.5) {
            result = delete();
            if (!result) {
              log.fatal("BigSortedMap allocation < 0.5");
              Thread.dumpStack();
              System.exit(-1);
            }
            assertTrue(result);
          } else if (d < 0.9) {
            // 0.9 is for safety, not all up to totalLoaded
            // can be really loaded (last ones may be in flight)
            int n = r.nextInt((int) (totalLoaded.get() * 0.9));
            long size = get(n);
            if (size < 0 && n > totalDeleted.get()) {
              fail("Not found " + n + " deleted=" + totalDeleted.get());
            }
            if (size > 0) {
              assertTrue(
                  Thread.currentThread().getName()
                      + " ERROR  num="
                      + i
                      + " totalLoaded="
                      + totalLoaded.get()
                      + " size="
                      + size,
                  size >= valueSizeDistribution[0]
                      && size <= valueSizeDistribution[valueSizeDistribution.length - 1]);
            }
          } else {
            int n = r.nextInt((int) (totalLoaded.get() * 0.9));
            scan(n);
          }
        }
        if (i % 1000000 == 0) {
          log.debug(
              "{}- {} allocated={} data={} index={} max={}",
              Thread.currentThread().getName(),
              i,
              BigSortedMap.getGlobalAllocatedMemory(),
              BigSortedMap.getGlobalDataSize(),
              BigSortedMap.getGlobalIndexSize(),
              BigSortedMap.getGlobalMemoryLimit());
        }
      }

      long end = System.currentTimeMillis();
      totalOps = num;
      log.debug("Time to get {} ={}ms", num, end - start);
      try {
        scanAll();
      } catch (InterruptedException | BrokenBarrierException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
    }

    private void getKey(long v) {
      r.setSeed(v);
      r.nextBytes(keyBuf);
      UnsafeAccess.copy(keyBuf, 0, key, keySize);
    }

    private int getValueSize() {
      double d = r.nextDouble();
      int size;
      int start, end;
      double range, diff;
      if (d < 0.5) {
        start = valueSizeDistribution[0];
        end = valueSizeDistribution[1];
        range = 0.5;
        diff = d - 0;
      } else if (d < 0.9) {
        start = valueSizeDistribution[1];
        end = valueSizeDistribution[2];
        range = 0.4;
        diff = d - 0.5;
      } else if (d < 0.95) {
        start = valueSizeDistribution[2];
        end = valueSizeDistribution[3];
        range = 0.05;
        diff = d - 0.9;
      } else if (d < 0.99) {
        start = valueSizeDistribution[3];
        end = valueSizeDistribution[4];
        range = 0.04;
        diff = d - 0.95;
      } else {
        start = valueSizeDistribution[4];
        end = valueSizeDistribution[5];
        range = 0.01;
        diff = d - 0.99;
      }
      size = (int) ((start * (range - diff) + end * diff) / range);
      return size;
    }

    private void runPuts() {
      log.debug("{}-test PUTs", Thread.currentThread().getName());
      long start = System.currentTimeMillis();
      long totalSize = 0;
      while (true) {
        if (BigSortedMap.getGlobalAllocatedMemory() > 0.90 * BigSortedMap.getGlobalMemoryLimit()) {
          break;
        }
        long n = totalLoaded.incrementAndGet();
        getKey(n);
        int valueSize = getValueSize();
        totalSize += keySize + valueSize;
        boolean res = map.put(key, keySize, value, valueSize, 0);
        if (!res) {
          totalLoaded.decrementAndGet();
          break;
        }

        totalOps++;
        if (n % 1000000 == 0) {
          log.debug("{}- {}", Thread.currentThread().getName(), n);
        }
      }

      long end = System.currentTimeMillis();
      log.debug("Time to put {} = {} ms. Total size={}", totalOps, end - start, totalSize);
    }

    private boolean put() {
      long n = totalLoaded.incrementAndGet();
      getKey(n);
      int valueSize = getValueSize();
      return map.put(key, keySize, value, valueSize, 0);
    }

    private boolean overwrite() {
      long n = totalLoaded.get();
      long k = totalDeleted.get();
      int m = r.nextInt((int) (n - k));
      m += k;
      getKey(m);
      int valueSize = getValueSize();
      return map.put(key, keySize, value, valueSize, 0);
    }

    private boolean delete() {
      long n = totalDeleted.incrementAndGet();
      getKey(n);
      return map.delete(key, keySize);
    }

    private long get(long n) {
      getKey(n);
      return map.get(key, keySize, value, maxValueSize, Long.MAX_VALUE);
    }

    /*
     * Short scan operation
     */
    private void scan(long n) throws IOException {
      getKey(n);
      int toScan = 221;

      BigSortedMapScanner scanner = map.getScanner(key, keySize, 0, 0);
      int count = 0;
      long prev = UnsafeAccess.malloc(keySize);
      long current = UnsafeAccess.malloc(keySize);
      while (scanner.hasNext() && count++ < toScan) {
        scanner.key(current, keySize);
        if (count > 1) {
          int result = Utils.compareTo(current, keySize, prev, keySize);
          assertTrue(result > 0);
        }
        UnsafeAccess.copy(current, prev, keySize);
        scanner.next();
      }
      scanner.close();
      UnsafeAccess.free(current);
      if (prev != current) {
        UnsafeAccess.free(prev);
      }
    }

    private void scanAll() throws IOException, InterruptedException, BrokenBarrierException {
      barrier.await();
      if (Thread.currentThread().getName().equals("0")) {
        barrier.reset();
      }

      BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
      int count = 0;
      long prev = UnsafeAccess.malloc(keySize);
      long prevVersion = 0;
      Op prevType = null;
      long current = UnsafeAccess.malloc(keySize);
      long start = System.currentTimeMillis();
      while (scanner.hasNext()) {
        count++;
        scanner.key(current, keySize);
        if (count > 1) {
          int result = Utils.compareTo(current, keySize, prev, keySize);
          if (result <= 0) {
            log.debug(
                "{} prevVersion={} prevType={} curVersion={} curType={}",
                result,
                prevVersion,
                prevType,
                scanner.keyVersion(),
                scanner.keyOpType());
          }
          assertTrue(result > 0);
        }
        prevVersion = scanner.keyVersion();
        prevType = scanner.keyOpType();
        UnsafeAccess.copy(current, prev, keySize);
        scanner.next();
      }
      scanner.close();
      UnsafeAccess.free(current);
      if (prev != current) {
        UnsafeAccess.free(prev);
      }
      long end = System.currentTimeMillis();
      log.debug("{} scanned {} in {}ms", Thread.currentThread().getName(), count, end - start);
    }
  }

  @Test
  public void testPerf() {

    BigSortedMap.setMaxBlockSize(4096);
    // int numThreads = 8;

    int totalCycles = 1000;
    int cycle = 0;
    while (cycle++ < totalCycles) {
      totalLoaded.set(0);
      totalDeleted.set(0);
      log.debug("LOOP={}", cycle);
      BigSortedMap map = new BigSortedMap((long) 1024 * 1024 * 1024);

      Worker[] workers = new Worker[totalThreads];
      for (int i = 0; i < totalThreads; i++) {
        workers[i] = new Worker(map, getName(i));
        workers[i].start();
      }

      for (int i = 0; i < totalThreads; i++) {
        try {
          workers[i].join();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          log.error("StackTrace: ", e);
        }
      }

      log.debug(
          "MEM={}\nDATA={}\nUTILIZATION={}",
          BigSortedMap.getGlobalAllocatedMemory(),
          BigSortedMap.getGlobalDataSize(),
          (double) BigSortedMap.getGlobalDataSize() / BigSortedMap.getGlobalAllocatedMemory());
      log.debug(
          "num threads={} PUT={} GET={} SCAN={}",
          totalThreads,
          putsPs.get(),
          comboPs.get(),
          scanPs.get());
      map.dispose();
      putsPs.set(0);
      comboPs.set(0);
      scanPs.set(0);
    }
  }

  private String getName(int i) {
    return Integer.toString(i);
  }
}
