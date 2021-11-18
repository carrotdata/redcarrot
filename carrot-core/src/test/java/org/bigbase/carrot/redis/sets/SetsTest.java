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
package org.bigbase.carrot.redis.sets;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.bigbase.carrot.util.Value;
import org.junit.Ignore;
import org.junit.Test;

public class SetsTest {

  private static final Logger log = LogManager.getLogger(SetsTest.class);

  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int valSize = 16;
  long n = 1000000;
  List<Value> values;

  static {
    // UnsafeAccess.debug = true;
  }

  private List<Value> getValues(long n) {
    List<Value> values = new ArrayList<Value>();
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
    List<Value> values = new ArrayList<Value>();
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

  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    values = getValues(n);
  }

  @Test
  public void testMultiAdd() {
    log.debug("Test multi add");
    map = new BigSortedMap(1000000000);
    values = getRandomValues(1000);

    List<Value> copy = new ArrayList<Value>();
    values.stream().forEach(x -> copy.add(x));

    Key key = getKey();
    int num = Sets.SADD(map, key.address, key.length, copy);
    assertEquals(values.size(), num);
    assertEquals(values.size(), (int) Sets.SCARD(map, key.address, key.length));
    for (Value v : values) {
      int result = Sets.SISMEMBER(map, key.address, key.length, v.address, v.length);
      assertEquals(1, result);
    }
    List<byte[]> members = Sets.SMEMBERS(map, key.address, key.length, values.size() * valSize * 2);
    assert members != null;
    for (byte[] v : members) {
      log.debug("{}", Bytes.toHex(v));
    }
    tearDown();
  }

  // @Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {} Compression=NULL", i + 1);
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }

  // @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    log.debug("");
    for (int i = 0; i < 1; i++) {
      log.debug("*************** RUN = {} Compression=LZ4", i + 1);
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }

  @Ignore
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    log.debug("");
    for (int i = 0; i < 10; i++) {
      log.debug("*************** RUN = {} Compression=LZ4HC", i + 1);
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }

  private void allTests() throws IOException {
    long start = System.currentTimeMillis();
    setUp();
    testSADDSISMEMBER();
    tearDown();
    setUp();
    testAddRemove();
    tearDown();
    setUp();
    testAddMultiDelete();
    tearDown();
    long end = System.currentTimeMillis();
    log.debug("\nRUN in {}ma", end - start);
  }

  @Ignore
  public void testPerformance() {
    perfRun(1000000);
    perfRun(10000000);
    perfRun(100000000);
  }

  private void perfRun(int n) {
    log.debug("Performance test run with {}", n);
    map = new BigSortedMap(10000000000L);

    int toQuery = 1000000;
    int nn = n > 10000000 ? 10000000 : n;

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

    Runnable run =
        new Runnable() {
          public void run() {
            Random r = new Random();
            for (int i = 0; i < toQuery; i++) {
              int index = r.nextInt(values.size());
              Value v = values.get(index);
              int res = Sets.SISMEMBER(map, key.address, key.length, v.address, v.length);
              assertEquals(1, res);
            }
          }
        };
    // runRead(1, run, toQuery);
    // runRead(2, run, toQuery);
    // runRead(4, run, toQuery);
    // runRead(8, run, toQuery);
    runRead(16, run, toQuery);

    log.debug("Skip List Map Size={}", map.getMap().size());
    tearDown();
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
    log.debug(
        "{} threads READ perf={} RPS",
        numThreads,
        +(double) numThreads * toQuery * 1000 / (end - start));
  }

  @Ignore
  @Test
  public void testSADDSISMEMBER() {
    log.debug("Test SADDSISMEMBER");
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
        BigSortedMap.getGlobalAllocatedMemory(),
        n,
        valSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize,
        +end - start);

    BigSortedMap.printGlobalMemoryAllocationStats();

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
    log.debug("Time exist={}ms", end - start);
    BigSortedMap.printGlobalMemoryAllocationStats();
    Sets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Sets.SCARD(map, key.address, key.length));
  }

  @Ignore
  @Test
  public void testAddMultiDelete() throws IOException {
    log.debug("Test Add Multi Delete");
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
        "Total allocated memory ="
            + BigSortedMap.getGlobalAllocatedMemory()
            + " for "
            + n
            + " "
            + valSize
            + " byte values. Overhead="
            + ((double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize)
            + " bytes per value. Time to load: "
            + (end - start)
            + "ms");

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
    BigSortedMap.printGlobalMemoryAllocationStats();
  }

  @Ignore
  @Test
  public void testAddRemove() {
    log.debug("Test Add Remove");
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
    log.debug(
        "Total allocated memory ="
            + BigSortedMap.getGlobalAllocatedMemory()
            + " for "
            + n
            + " "
            + valSize
            + " byte values. Overhead="
            + ((double) BigSortedMap.getGlobalAllocatedMemory() / n - valSize)
            + " bytes per value. Time to load: "
            + (end - start)
            + "ms");

    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, Sets.SCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int res =
          Sets.SREM(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    log.debug("Time exist={}ms", end - start);
    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(0, (int) map.countRecords());
    assertEquals(0, (int) Sets.SCARD(map, key.address, key.length));
    // TODO
    Sets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Sets.SCARD(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
  }

  @Ignore
  @Test
  public void testMemoryUsageForInts() throws IOException {
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
        continue;
      }
    }
    long end = System.currentTimeMillis();
    log.debug(
        "Loaded in "
            + (end - start)
            + "ms. Mem usage for 1 int="
            + ((double) BigSortedMap.getGlobalAllocatedMemory()) / n
            + " dups="
            + duplicates);

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
    List<Integer> list = new ArrayList<Integer>();
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
    long cBuffer = UnsafeAccess.malloc(2 * bufferSize);
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

  private void tearDown() {
    // Dispose
    map.dispose();
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
