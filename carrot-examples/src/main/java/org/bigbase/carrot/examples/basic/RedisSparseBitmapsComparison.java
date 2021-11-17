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
package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

/**
 * The Test runs sparse bitmap tests in Redis with different population count = 0.01
 *
 * <p>Results (Carrot sparse bitmaps with LZ4HC compression vs. Redis bitmaps):
 *
 * <p>Bitmap size = 100,000,000 sizeUncompressedBitmap = 12,500,000 bytes Test_consumed_RAM ~
 * 13,311,264 (Redis)
 *
 * <p>COMPRESSION
 *
 * <p>population CARROT LZ4HC REDIS count (dencity)
 *
 * <p>dencity=0.01 4.2 0.94
 *
 * <p>Carrot/Redis = 4.5
 *
 * <p>Notes: COMPRESSION = sizeUncompressedBitmap/Test_consumed_RAM
 *
 * <p>sizeUncompressedBitmap - size of an uncompressed bitmap, which can hold all the bits
 * Test_consumed_RAM - RAM consumed by test.
 */
public class RedisSparseBitmapsComparison {

  private static final Logger log = LogManager.getLogger(RedisSparseBitmapsComparison.class);

  static int bufferSize = 64;
  static int keySize = 8;
  static int N = 1000000;
  static int delta = 100;
  static double dencity = 0.01;

  static double[] dencities = new double[] {0.01};

  private static void testPerformance() throws IOException {
    Jedis client = new Jedis("localhost");

    log.debug("\nTest Redis Performance sparse bitmaps. dencity={}\n", dencity);
    long offset = 0;
    long MAX = (long) (N / dencity);
    Random r = new Random();

    long start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      offset = Math.abs(r.nextLong()) % MAX;
      client.setbit("key", offset, true);
      if (i % 10000 == 0 && i > 0) {
        log.debug("i={}", i);
      }
    }
    long end = System.currentTimeMillis();

    log.debug(
        "Time for {} population dencity={} bitmap size={} new SetBit={}ms",
        +N,
        dencity,
        MAX,
        end - start);

    log.debug("Press any button ...");
    System.in.read();
    client.del("key");
    client.close();
  }

  public static void main(String[] args) throws IOException {
    testPerformance();
  }
}
