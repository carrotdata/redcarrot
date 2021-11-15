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
package org.bigbase.carrot.examples.geoip;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

/**
 * RedisBook GeoIP example
 *
 * <p>Total memory usage: 388.5 MB
 */
public class TestRedisGeoIP {

  private static final Logger log = LogManager.getLogger(TestRedisGeoIP.class);

  static List<CityBlock> blockList;
  static List<CityLocation> locList;
  static Jedis client;

  public static void main(String[] args) throws IOException {
    client = new Jedis("localhost");
    runTest(args[0], args[1]);
  }

  private static void runTest(String f1, String f2) throws IOException {
    if (blockList == null) {
      blockList = CityBlock.load(f1);
    }
    byte[] key = "key1".getBytes();
    long start = System.currentTimeMillis();
    int total = 0;
    for (CityBlock cb : blockList) {
      cb.saveToRedis(client, key);
      total++;
      if (total % 100000 == 0) {
        log.debug("Total blocks=" + total);
      }
    }
    long end = System.currentTimeMillis();

    log.debug("Loaded " + blockList.size() + " blocks in " + (end - start) + "ms");
    total = 0;
    if (locList == null) {
      locList = CityLocation.load(f2);
    }
    start = System.currentTimeMillis();
    for (CityLocation cl : locList) {
      cl.saveToRedis(client);
      total++;
      if (total % 100000 == 0) {
        log.debug("Total locs=" + total);
      }
    }
    end = System.currentTimeMillis();

    log.debug("Loaded " + locList.size() + " locations in " + (end - start) + "ms");
    log.debug("Press any button ...");
    System.in.read();
    client.del(key);

    for (CityLocation cl : locList) {
      byte[] k = cl.getKey();
      client.del(k);
    }
  }
}
