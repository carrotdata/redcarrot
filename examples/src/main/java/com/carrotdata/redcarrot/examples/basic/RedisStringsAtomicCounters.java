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
package com.carrotdata.redcarrot.examples.basic;

import java.io.IOException;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Strings.INCRBY and Strings.INCRBYFLOAT to keep huge list of
 * atomic counters Test Description:
 *
 * <p>Key format: "counter:number" number = [0:1M]
 *
 * <p>1. Load 1M long and double counters 2. Increment each by random number between 1:1000 3.
 * Calculate Memory usage
 *
 * <p>Results:
 *
 * <p>1. Average counter size is 21 (13 bytes - key, 8 - value) 2. Carrot No compression. 37.5 bytes
 * per counter 3. Carrot LZ4 - 10.8 bytes per counter 4. Carrot LZ4HC - 10.3 bytes per counter 5.
 * Redis memory usage per counter is 57.5 bytes
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 57.5/37.5 ~ 1.5x 2) LZ4 compression 57.5/10.8 ~ 5.3x 3) LZ4HC compression
 * 57.5/10.3 ~ 5.6x
 */
public class RedisStringsAtomicCounters {

  private static final Logger log = LogManager.getLogger(RedisStringsAtomicCounters.class);

  static long N = 1000000;
  static long totalDataSize = 0;
  static int MAX_VALUE = 1000;

  public static void main(String[] args) throws IOException, OperationFailedException {

    log.debug("RUN Redis");
    runTest();
  }

  private static void runTest() throws IOException, OperationFailedException {
    log.debug("Running Redis Strings test ...");

    Jedis client = new Jedis("localhost");
    totalDataSize = 0;

    long startTime = System.currentTimeMillis();
    Random r = new Random();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      totalDataSize += skey.length() + 8 /*length of a counter*/;
      client.incrBy(skey, nextScoreSkewed(r));
      if (i % 10000 == 0 && i > 0) {
        log.debug("set string :{}", i);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} counters, total size={} in{}ms", N, totalDataSize, endTime - startTime);

    log.debug("Press any button ...");
    System.in.read();
    deleteAll(client);
    client.close();
  }

  private static void deleteAll(Jedis client) {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      client.del(skey);
      if (i % 10000 == 0 && i > 0) {
        log.debug("del {}", i);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Deleted {} counters in {}ms", N, endTime - startTime);
  }

  private static int nextScoreSkewed(Random r) {
    double d = r.nextDouble();
    return (int) Math.rint(d * d * d * d * d * MAX_VALUE);
  }

  private static int nextScore(Random r) {
    return r.nextInt(MAX_VALUE);
  }
}
