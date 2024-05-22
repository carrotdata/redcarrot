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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.examples.util.Address;
import com.carrotdata.redcarrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Hashes to store user Address objects Data set is the random
 * sample of a openaddress.org data set for US-WEST region
 *
 * <p>File: all-clean-sub-shuffle.csv
 *
 * <p>User Address structure:
 *
 * <p>"LON" - Address longitude (skip) "LAT" - Address latitude (skip) "NUMBER" - House number
 * "STREET" - Street "UNIT" - Unit number "CITY" - City "DISTRICT" - Region "REGION" - Region
 * (State)
 *
 * <p>Test description: <br>
 * Address object has up to 6 fields (some of them can be missing) Key is synthetic:
 * "address:user:number" <- format of a key
 *
 * <p>Average key + address object size is 86 bytes. We load 413689 user address objects
 *
 * <p>Results: 0. Average user address data size = 86 bytes 1. No compression. Used RAM per address
 * object is 124 bytes (COMPRESSION= 0.7) 2. LZ4 compression. Used RAM per address object is 66
 * bytes (COMPRESSION = 1.3) 3. LZ4HC compression. Used RAM per address object is 63.6 bytes
 * (COMPRESSION = 1.35)
 *
 * <p>Redis, per address object, using Hashes with ziplist encodings (most efficient) is 193
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 193/124 = 1.6x 2) LZ4 compression 193/66 = 2.9x 3) LZ4HC compression
 * 193/63.6 = 3.0x
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 1.3/0.7 = 1.86x (to no compression) LZ4HC - 1.35/0.7 = 1.93x (to no compression)
 */
public class RedisHashesAddresses {

  private static final Logger log = LogManager.getLogger(RedisHashesAddresses.class);

  static long totalDataSize = 0;
  static List<Address> addressList;

  public static void main(String[] args) throws IOException, OperationFailedException {
    addressList = Address.loadFromFile(args[0]);
    log.debug("RUN Redis ");
    runTest();
  }

  private static void runTest() throws IOException, OperationFailedException {

    Jedis client = new Jedis("localhost");

    long startTime = System.currentTimeMillis();
    int count = 0;

    for (Address us : addressList) {
      count++;
      String skey = Address.getUserId(count);
      Map<String, String> map = us.getPropsMap();
      if (count % 10000 == 0) {
        log.debug("set {}", count);
      }
      client.hset(skey, map);
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} user address objects in {}ms", addressList.size(), endTime - startTime);
    client.close();

    log.debug("Press any button ...");
    System.in.read();

    count = 0;
    int listSize = addressList.size();
    for (int i = 0; i < listSize; i++) {
      count++;
      String skey = Address.getUserId(count);
      if (count % 10000 == 0) {
        log.debug("del {}", count);
      }
      client.del(skey);
    }
  }
}
