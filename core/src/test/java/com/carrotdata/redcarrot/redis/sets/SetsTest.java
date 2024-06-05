/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.sets;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.CarrotCoreBase;
import com.carrotdata.redcarrot.compression.Codec;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.redis.util.Commons;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;
import com.carrotdata.redcarrot.util.Value;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class SetsTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(SetsTest.class);

  Key key;
  long buffer;
  int bufferSize = 64;
  int valSize = 16;
  long n = 100000;
  List<Value> values;

  public SetsTest(Object c) {
    super(c);
    n = memoryDebug ? 10000 : 100000;
  }

  private List<Value> getValues(long n) {
    values = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("VALUES SEED={}", seed);
    byte[] buf = new byte[valSize / 2];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      UnsafeAccess.copy(buf, 0, ptr + buf.length, buf.length);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }

  private List<Value> getRandomValues(long n) {
    values = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("VALUES SEED={}", seed);
    byte[] buf = new byte[valSize];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }

  private Key getKey() {
    long ptr = UnsafeAccess.malloc(valSize);
    byte[] buf = new byte[valSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("KEY SEED={}", seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, valSize);
    return key = new Key(ptr, valSize);
  }

  @Before
  public void setUp() throws IOException {
    super.setUp();

    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    values = getValues(n);
  }

  @Ignore
  @Test
  public void untestPerformance1M() {
    perfRun(1000000);
  }

  @Ignore
  @Test
  public void untestPerformance10M() {
    perfRun(1000000);
  }

  @Ignore
  @Test
  public void untestPerformance100M() {
    perfRun(100000000);
  }

  private void perfRun(int n) {
    int toQuery = 1000000;
    int nn = Math.min(n, 10000000);

    values = getRandomValues(nn);
    Key key = getKey();
    long start = System.currentTimeMillis();
    int count = 0;
    for (Value v : values) {
      int res = Sets.SADD(map, key.address, key.length, v.address, v.length);
      assertEquals(1, res);
      count++;
      if (count % 1000000 == 0) {
        log.debug("Loaded {}", count);
      }
    }
    Random r = new Random();
    if (n > nn) {
      byte[] buf = new byte[valSize];
      for (int i = nn; i < n; i++) {
        r.nextBytes(buf);
        long ptr = UnsafeAccess.allocAndCopy(buf, 0, buf.length);
        int size = buf.length;
        Sets.SADD(map, key.address, key.length, ptr, size);
        UnsafeAccess.free(ptr);
        if ((i > nn) && (i % 1000000 == 0)) {
          log.debug("Loaded {}", i);
        }
      }
    }
    long end = System.currentTimeMillis();

    log.debug("{} items: load={} RPS", n, (double) n * 1000 / (end - start));
    assertEquals(n, (int) Sets.SCARD(map, key.address, key.length));

    Runnable run = () -> {
      Random r1 = new Random();
      for (int i = 0; i < toQuery; i++) {
        int index = r1.nextInt(values.size());
        Value v = values.get(index);
        int res = Sets.SISMEMBER(map, key.address, key.length, v.address, v.length);
        assertEquals(1, res);
      }
    };
    // runRead(1, run, toQuery);
    // runRead(2, run, toQuery);
    // runRead(4, run, toQuery);
    // runRead(8, run, toQuery);
    runRead(16, run, toQuery);

    log.debug("Skip List Map Size={}", map.getMap().size());
  }

  private void runRead(int numThreads, Runnable run, int toQuery) {

    long start = System.currentTimeMillis();
    Thread[] pool = new Thread[numThreads];
    for (int i = 0; i < pool.length; i++) {
      pool[i] = new Thread(run);
      pool[i].start();
    }

    for (Thread t : pool) {
      try {
        t.join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("{} threads READ perf={} RPS", numThreads,
      (double) numThreads * toQuery * 1000 / (end - start));
  }

  @Test
  public void testSADDSISMEMBER() {

    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      int num = Sets.SADD(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, num);
      if (++count % 100000 == 0) {
        log.debug("add {}", count);
      }
    }
    long end = System.currentTimeMillis();
    log.debug(
      "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per value. Time to load: {}ms",
      BigSortedMap.getGlobalAllocatedMemory(), n, valSize,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize, end - start);

    assertEquals(n, Sets.SCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    count = 0;
    for (int i = 0; i < n; i++) {
      int res =
          Sets.SISMEMBER(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
      if (++count % 100000 == 0) {
        log.debug("ismember {}", count);
      }
    }
    end = System.currentTimeMillis();
    log.debug("Time exist={} ms", end - start);
    Sets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Sets.SCARD(map, key.address, key.length));
  }

  @Test
  public void testAddMultiDelete() throws IOException {

    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      int num = Sets.SADD(map, elemPtrs[0], elemSizes[0], elemPtrs, elemSizes);
      assertEquals(1, num);
      if (++count % 100000 == 0) {
        log.debug("add {}", count);
      }
    }
    long end = System.currentTimeMillis();
    log.debug(
      "Total allocated memory = {} for {} {}  byte values. Overhead={} bytes per value. Time to load: {}ms",
      BigSortedMap.getGlobalAllocatedMemory(), n, valSize,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize, end - start);

    log.debug("Deleting keys ...");
    count = 0;
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      Sets.DELETE(map, elemPtrs[0], elemSizes[0]);
      if (++count % 100000 == 0) {
        log.debug("delete {}", count);
      }
    }
    end = System.currentTimeMillis();
    long recc = Commons.countRecords(map);

    log.debug("Deleted {} in {}ms. Count={}", n, end - start, recc);

    assertEquals(0, (int) recc);
  }

  @Test
  public void testAddRemove() {

    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      int num = Sets.SADD(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, num);
      if (++count % 100000 == 0) {
        log.debug("add {}", count);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Total allocated memory =" + BigSortedMap.getGlobalAllocatedMemory() + " for " + n
        + " " + valSize + " byte values. Overhead="
        + ((double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize)
        + " bytes per value. Time to load: " + (end - start) + "ms");

    assertEquals(n, Sets.SCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int res =
          Sets.SREM(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    log.debug("Time exist= {}ms", end - start);

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Sets.SCARD(map, key.address, key.length));
    // TODO
    Sets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Sets.SCARD(map, key.address, key.length));
  }

  @Ignore
  @Test
  public void testMemoryUsageForInts() {
    log.debug("Test memory usage for ints");

    int n = 1000000;
    map = new BigSortedMap(100000000);
    long buffer = UnsafeAccess.malloc(Utils.SIZEOF_INT);
    long keyPtr = UnsafeAccess.allocAndCopy("key", 0, "key".length());
    int keySize = "key".length();
    Random r = new Random();
    int duplicates = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int next = Math.abs(r.nextInt());
      UnsafeAccess.putInt(buffer, next);
      int res = Sets.SADD(map, keyPtr, keySize, buffer, Utils.SIZEOF_INT);
      if (res == 0) {
        duplicates++;
        i--;
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Loaded in {}ms. Mem usage for 1 int={} dups={}", end - start,
      (double) BigSortedMap.getGlobalAllocatedMemory() / n, duplicates);

    BigSortedMap.printGlobalMemoryAllocationStats();
    map.dumpStats();
    assertEquals(n, (int) Sets.SCARD(map, keyPtr, keySize));
    map.dispose();
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
  }

  @Ignore
  @Test
  public void testCompressionSortedIntSet() throws IOException {
    log.debug("Test compression sorted int set");

    int n = 1000000;
    List<Integer> list = new ArrayList<>();
    Random r = new Random();
    while (list.size() < n) {
      int v = Math.abs(r.nextInt());
      // if (list.contains(v)) continue;
      list.add(v);
    }

    Collections.sort(list);
    int bufferSize = (Utils.SIZEOF_INT + Utils.sizeUVInt(Utils.SIZEOF_INT)) * n;
    long buffer = UnsafeAccess.malloc(bufferSize);
    long ptr = buffer;
    for (int v : list) {
      Utils.writeUVInt(ptr, Utils.SIZEOF_INT);
      int mSizeSize = Utils.sizeUVInt(Utils.SIZEOF_INT);
      UnsafeAccess.putInt(ptr + mSizeSize, v);
      ptr += Utils.SIZEOF_INT + mSizeSize;
    }

    byte[] arr = new byte[bufferSize];
    UnsafeAccess.copy(buffer, arr, 0, bufferSize);
    // Compress arr using LZ4 codec
    long cBuffer = UnsafeAccess.malloc(2L * bufferSize);
    Codec codec = CodecFactory.getInstance().getCodec(CodecType.LZ4HC);
    int size = codec.compress(buffer, bufferSize, cBuffer, 2 * bufferSize);
    log.debug("Source size ={}", bufferSize);
    log.debug("LZ4HC  size ={}", size);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream os = new GZIPOutputStream(baos);
    os.write(arr);
    os.close();
    log.debug("GZIP   size ={}", baos.toByteArray().length);

    Path path = Files.createTempFile("data", "raw");
    File f = path.toFile();

    log.debug("File={}", f.getAbsolutePath());
    FileOutputStream fos = new FileOutputStream(f);
    fos.write(arr);
    fos.close();
  }

  @Override
  public void extTearDown() {

    if (key != null) {
      UnsafeAccess.free(key.address);
      key = null;
    }
    for (Value v : values) {
      UnsafeAccess.free(v.address);
    }
    if (buffer > 0) {
      UnsafeAccess.free(buffer);
      buffer = 0;
    }
  }
}
