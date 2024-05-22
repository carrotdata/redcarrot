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
package com.carrotdata.redcarrot.examples.appcomps;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

/**
 * Counters and statistics. Redis Book, Chapter 5.1:
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-5-using-redis-for-application-support/
 * 5-2-counters-and-statistics/5-2-2-storing-statistics-in-redis/
 *
 * <p>We implement simple application which stores Web-application's page access time statistics.
 * The app is described in the Redis book (see the link above)
 *
 * <p>We collect hourly statistics for 1 year on web page access time
 *
 * <p>The key = "stats:profilepage:access:hour"
 *
 * <p>The key consists from several parts:
 *
 * <p>stats - This is top group name - means "Statistics" profilepage - page we colect statistics on
 * access - statistics on total access time hour - 8 byte timestamp for the hour
 *
 * <p>There are 24*365 = 8,760 hours in a year, so there are 8,760 keys in the application. For
 * Redis we will use ordered sets (ZSET) as recommended in the book), for Carrot we will use HASH
 * type to store the data.
 *
 * <p>We collect the following statistics:
 *
 * <p>"min" - minimum access time "max" - maximum access time "count" - total number of accesses
 * "sum" - sum of access times "sumsq" - sum of squares of access time
 *
 * <p>The above info information will allow us to calculate std deviation, min, max, average, total.
 */
public class TestRedisAppStats {

  private static final Logger log = LogManager.getLogger(TestRedisAppStats.class);

  static final String KEY_PREFIX = "stats:profilepage:access:";
  static final int hoursToKeep = 10 * 365 * 24;

  public static void main(String[] args) throws IOException {
    runTest();
  }

  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");

    client.flushAll();

    long start = System.currentTimeMillis();
    for (int i = 0; i < hoursToKeep; i++) {
      Stats st = Stats.newStats(i);
      st.saveToRedisNative(client);
      byte[] key = st.getKeyBytes();
      // client.expire(key, 100000);
      if ((i + 1) % 100000 == 0) {
        log.debug("loaded {}", i + 1);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Loaded {} {}ms. Press any button ...", hoursToKeep, end - start);
    start = System.currentTimeMillis();
    client.save();
    end = System.currentTimeMillis();
    log.debug("save time={}ms", end - start);
    System.in.read();
    client.close();
  }
}
