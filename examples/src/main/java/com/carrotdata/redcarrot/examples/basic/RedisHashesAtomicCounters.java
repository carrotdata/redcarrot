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
 * This example shows how to use Redis Hashes.HINCRBY to keep huge list of atomic counters
 *
 * <p>Test Description:
 *
 * <p>Key format: "counter:number" number = [0:1M]
 *
 * <p>1. Load 1M long and double counters 2. Increment each by random number between 1:1000 3.
 * Calculate Memory usage
 *
 * <p>Redis
 *
 * <p>In Redis Hashes with ziplist encodings can be used to keep counters TODO: we need to compare
 * Redis optimized version with our default
 *
 * <p>Hashes:
 *
 * <p>Redis uses hashes to minimize RAM usage
 *
 * <p>key="counter:"+ num;
 *
 * <p>Hash key = Math.max(8, key.length - 3) // we use last 3 chars (digits) as a field name in a
 * hash
 *
 * <p>Redis usage is 8.3 bytes per counter for normal score distribution and 7.3 for skewed
 * distribution
 *
 * <p>This is still greater than Carrot with LZ4, LZ4HC compression enabled:
 *
 * <p>Normal distribution:
 *
 * <p>Redis = 8.3 bytes per counter Carrot No compression = 8.8 Carrot LZ4 = 7.2 Carrot LZ4HC = 7.1
 *
 * <p>Skewed distribution:
 *
 * <p>Redis = 7.3 bytes per counter Carrot No compression = 7.7 Carrot LZ4 = 6.5 Carrot LZ4HC = 6.4
 *
 * <p>Overall we still see 20-25% RAM efficiency over Redis with Carrot LZ4/LZ4HC
 */
public class RedisHashesAtomicCounters {

  private static final Logger log = LogManager.getLogger(RedisHashesAtomicCounters.class);

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
    log.debug("Running Redis Hashes test ...");
    Random r = new Random();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;

      int keySize = Math.max(8, skey.length() - 2);
      String key = skey.substring(0, keySize);
      String field = skey.substring(keySize);

      client.hincrBy(key, field, nextScoreSkewed(r));
      if (i % 10000 == 0 && i > 0) {
        log.debug("set hash {}", i);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug(
        "Loaded {} counters into hash, total size={} in {}ms",
        N,
        totalDataSize,
        endTime - startTime);

    log.debug("Press any button ...");
    System.in.read();

    deleteAllHash(client);
    client.close();
  }

  private static void deleteAllHash(Jedis client) {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      int keySize = Math.max(8, skey.length() - 3);
      skey = skey.substring(0, keySize);
      client.del(skey);
      if (i % 10000 == 0 && i > 0) {
        log.debug("del {}", i);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Deleted {} counters {}ms", N, endTime - startTime);
  }

  private static int nextScoreSkewed(Random r) {
    double d = r.nextDouble();
    return (int) Math.rint(d * d * d * d * d * MAX_VALUE);
  }

  private static int nextScore(Random r) {
    return r.nextInt(MAX_VALUE);
  }
}
