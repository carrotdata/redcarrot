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
package com.carrotdata.redcarrot.examples.basic;

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.sparse.SparseBitmaps;
import com.carrotdata.redcarrot.redis.util.Commons;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * Carrot sparse bitmaps can be more memory efficient then Redis bitmaps when bit population count
 * is small (< 0.05)
 * <p>
 * The Test runs sparse bitmap tests with different population counts and measure compression
 * relative to Redis bitmap.
 * <p>
 * Note:
 * <p>
 * We do not take into account how Redis allocate memory for bitmap
 * <p>
 * Result (for LZ4HC compression):
 * <p>
 * population COMPRESSION count (dencity)
 * <p>
 * dencity=1.0E-6 5993
 * <p>
 * dencity=1.0E-5 647
 * <p>
 * dencity=1.0E-4 118
 * <p>
 * dencity=0.001 27
 * <p>
 * dencity=0.01 4.2
 * <p>
 * dencity=0.02 2.5
 * <p>
 * dencity=0.03 2.0
 * <p>
 * dencity=0.04 1.6
 * <p>
 * dencity=0.05 1.43
 * <p>
 * dencity=0.075 1.2
 * <p>
 * dencity=0.1 1
 * <p>
 * Notes: COMPRESSION = sizeUncompressedBitmap/Test_consumed_RAM
 * <p>
 * sizeUncompressedBitmap - size of an uncompressed bitmap, which can hold all the bits
 * Test_consumed_RAM - RAM consumed by test.
 */
public class SparseBitmapsComparison {

  private static final Logger log = LogManager.getLogger(SparseBitmapsComparison.class);

  static BigSortedMap map;
  static Key key;
  static long buffer;
  static int bufferSize = 64;
  static int keySize = 8;
  static long N = 1000000;
  static int delta = 100;
  static double dencity = 0.01;

  static double[] dencities = new double[] {
      /* 0.000001, 0.00001, 0.0001, 0.001, */
      0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1 };

  static {
    // UnsafeAccess.debug = true;
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);

  }

  private static Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("SEED={}", seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }

  private static void setUp() {
    map = new BigSortedMap(10000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    key = getKey();
  }

  private static void tearDown() {
    map.dispose();

    UnsafeAccess.free(key.address);
    UnsafeAccess.free(buffer);
  }

  private static void runAllNoCompression() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    log.debug("");
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];
      log.debug("*************** RUN = {} Compression=NULL, dencity={}", i + 1, dencity);
      allTests();
    }
  }

  private static void runAllCompressionLZ4() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    log.debug("");
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];
      log.debug("*************** RUN = {} Compression=LZ4, dencity={}", i + 1, dencity);
      allTests();
    }
  }

  private static void runAllCompressionZSTD() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    log.debug("");
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];

      log.debug("*************** RUN = {} Compression=ZSTD, dencity={}", i + 1, dencity);
      allTests();
    }
  }

  private static void runAllCompressionLZ4HC() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    log.debug("");
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];

      log.debug("*************** RUN = {} Compression=LZ4HC, dencity={}", i + 1, dencity);
      allTests();
    }
  }

  private static void allTests() {
    setUp();
    testPerformance();
    tearDown();
  }

  private static void testPerformance() {

    log.debug("\nTest Performance Opt\n");
    long offset = 0;
    long MAX = (long) (N / dencity);
    Random r = new Random();

    long start = System.currentTimeMillis();
    long expected = N;
    for (int i = 0; i < N; i++) {
      offset = Math.abs(r.nextLong()) % MAX;
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (bit == 1) {
        expected--;
      }
    }
    long end = System.currentTimeMillis();
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    log.debug("Total RAM={} MAX={}\n", memory, MAX);

    long count =
        SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assert (expected == count);

    log.debug("Time for {} population dencity={} bitmap size={} new SetBit={}ms.", N, dencity, MAX,
      end - start);
    log.debug("COMPRESSION ratio ={}, memory={}", (double) MAX / (8 * memory), memory);
    BigSortedMap.printGlobalMemoryAllocationStats();
  }

  public static void main(String[] args) {
    runAllNoCompression();
    // runAllCompressionLZ4();
    // runAllCompressionLZ4HC();
    // runAllCompressionZSTD();

  }
}
