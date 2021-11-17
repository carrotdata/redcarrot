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
package org.bigbase.carrot.examples.adserver;

import java.io.IOException;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;

/**
 * ----- Data structures to keep user targeting:
 *
 * <p>7. UserActionWords: ZSET keeps user behavior userId -> {word,score} We record user actions for
 * every ad he/she acts on in the following way: if user acts on ad, we get the list of words
 * targeted by this ad and increment score for every word in the user's ordered set. 8.
 * UserViewWords: ZSET - the same as above but only for views (this data set is much bigger than in
 * 7.) 9. UserViewAds: HASH keeps history of all ads shown to a user during last XXX minutes, hours,
 * days. 10 UserActionAds: HASH keeps history of ads user clicked on during last XX minutes, hours,
 * days.
 *
 * <p>Results:
 *
 * <p>Redis 6.0.10 = 992,291,824 Carrot no compression = 254,331,520 Carrot LZ4 compression =
 * 234,738,048 Carrot LZ4HC compression = 227,527,936
 *
 * <p>Notes:
 *
 * <p>The test uses synthetic data, which is mostly random and not compressible
 */
public class TestRedisAdServerUserTarget {

  private static final Logger log = LogManager.getLogger(TestRedisAdServerUserTarget.class);

  static final int MAX_ADS = 10000;
  static final int MAX_WORDS = 10000;
  static final int MAX_USERS = 1000;

  public static void main(String[] args) throws IOException {
    runTest();
  }

  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");
    doUserViewWords(client);
    doUserActionWords(client);
    doUserViewAdds(client);
    doUserActionAdds(client);
    log.debug("Press any button ...");
    System.in.read();
    client.flushAll();
    client.close();
  }

  private static void doUserViewWords(Jedis client) {
    // SET
    log.debug("Loading UserViewWords data");
    String key = "user:viewwords:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_WORDS);
      String k = key + i;
      for (int j = 0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        client.zadd(k.getBytes(), r.nextDouble(), word.getBytes());
        count++;
        if (count % 100000 == 0) {
          log.debug("UserViewWords :{}", count);
        }
      }
    }
    long end = System.currentTimeMillis();
    log.debug("UserViewWords : loaded {} in {}ms", count, end - start);
  }

  private static void doUserActionWords(Jedis client) {
    // SET
    log.debug("Loading UserActionWords data");
    String key = "user:actionwords:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_WORDS / 100);
      String k = key + i;
      for (int j = 0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        client.zadd(k.getBytes(), r.nextDouble(), word.getBytes());
        count++;
        if (count % 100000 == 0) {
          log.debug("UserViewWords :{}", count);
        }
      }
    }
    long end = System.currentTimeMillis();
    log.debug("UserActionWords : loaded {} in {}ms", count, end - start);
  }

  private static void doUserViewAdds(Jedis client) {
    log.debug("Loading User View Ads data");
    String key = "user:viewads:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_ADS);
      String k = key + i;
      for (int j = 0; j < n; j++) {
        count++;
        int id = MAX_ADS - j;
        int views = r.nextInt(100);
        client.hset(k.getBytes(), Bytes.toBytes(id), Bytes.toBytes(views));
        if (count % 100000 == 0) {
          log.debug("UserViewAds :{}", count);
        }
      }
    }
    long end = System.currentTimeMillis();
    log.debug("UserViewAds : loaded {} in {}", count, end - start);
  }

  private static void doUserActionAdds(Jedis client) {
    log.debug("Loading User Action Ads data");
    String key = "user:actionads:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_ADS / 100);
      String k = key + i;
      for (int j = 0; j < n; j++) {
        count++;
        int id = MAX_ADS - j;
        int views = r.nextInt(100);
        client.hset(k.getBytes(), Bytes.toBytes(id), Bytes.toBytes(views));
        if (count % 100000 == 0) {
          log.debug("UserViewAds :{}", count);
        }
      }
    }
    long end = System.currentTimeMillis();
    log.debug("UserActionAds : loaded {} in {}ms", count, end - start);
  }
}
