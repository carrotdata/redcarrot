/**
 * Copyright (C) 2021-present Carrot, Inc.
 *
 * <p>This program is free software: you can redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by MongoDB, Inc.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * <p>You should have received a copy of the Server Side Public License along with this program. If
 * not, see <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package org.bigbase.carrot.examples.adserver;

import java.io.IOException;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;

import redis.clients.jedis.Jedis;

/**
 * ----- Data structures to keep ads performance:
 *
 * <p>11. AdSitePerf: HASH key = adId, {member1= siteId%'-V' value1 = views}, {member2= siteId%'-A'
 * value2 = actions}
 *
 * <p>For every Ad and every site, this data keeps total number of views and actions
 *
 * <p>12. AdSitesRank : ZSET key = adID, member = siteID, score = CTR (click-through-rate)
 *
 * <p>This data set allows to estimate performance of a given ad on a different sites.
 *
 * <p>Results:
 *
 * <p>Redis 6.0.10 = Carrot no compression = Carrot LZ4 compression = Carrot LZ4HC compression =
 *
 * <p>Notes:
 *
 * <p>The test uses synthetic data, which is mostly random and not compressible
 */
public class TestRedisAdServerAdsPerf {

  static final int MAX_ADS = 1000;
  static final int MAX_SITES = 10000;

  public static void main(String[] args) throws IOException {
    runTest();
  }

  private static void runTest() throws IOException {

    Jedis client = new Jedis("localhost");
    doAdsSitePerf(client);
    doAdsSiteRank(client);
    System.out.println("Press any button ...");
    System.in.read();
    client.flushAll();
    client.close();
  }

  private static void doAdsSiteRank(Jedis client) {
    System.out.println("Loading AdsSiteRank data");
    String key = "ads:sites:rank";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      String k = key + i;

      for (int j = 0; j < MAX_SITES; j++) {
        int siteId = j;
        client.zadd(k.getBytes(), r.nextDouble(), Bytes.toBytes(siteId));
        count++;
        if (count % 100000 == 0) {
          System.out.println("AdsSiteRank :" + count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("AdsSiteRank : loaded " + count + " in " + (end - start) + "ms");
  }

  private static void doAdsSitePerf(Jedis client) {
    System.out.println("Loading Ads-Site Performance data");
    String key = "ads:site:perf:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_ADS; i++) {
      String k = key + i + ":";
      for (int j = 0; j < MAX_SITES; j++) {
        // Optimize Redis Hash to use compact representation
        String kk = k + j / 100;
        int rem = j % 100;
        byte[] field1 = new byte[5];
        byte[] field2 = new byte[5];
        Bytes.putInt(field1, 0, rem);
        field1[4] = (byte) 0;
        Bytes.putInt(field2, 0, rem);
        field2[4] = (byte) 1;

        client.hset(kk.getBytes(), field1, Bytes.toBytes(r.nextInt(100)));
        client.hset(kk.getBytes(), field2, Bytes.toBytes(r.nextInt(100)));
        count++;
        if (count % 100000 == 0) {
          System.out.println("AdsSitePerf :" + count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("AdsSitePerf : loaded " + count + " in " + (end - start) + "ms");
  }
}
