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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Set to keep list of a REAL spam domains. This test uses Redis
 * Set to keep all data
 *
 * <p>File: spam_domains.txt.s
 *
 * <p>Test loads list of real spam domains (51672 records) into 10 different sets, total number of
 * loaded records is 516720 (51672 * 10)
 *
 * <p>RESULTS:
 *
 * <p>1. Raw size of all data is 7,200,570 bytes 2. Carrot NoCompression - RAM usage 8,566,630,
 * COMPRESSION = 0.84 3 Carrot LZ4 compression - RAM usage 6,657,078, COMPRESSION = 1.08 4. Carrot
 * LZ4HC compression - RAM usage 6,154,038, COMPRESSION = 1.17
 *
 * <p>LZ4 compression relative to NoCompression = 1.08/0.84 = 1.29 LZ4HC compression relative to
 * NoCompression = 1.17/0.84 = 1.4
 *
 * <p>Redis SET usage per object is 68.5 bytes. 35MB total
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 35M/8.5M ~ 4.1x 2) LZ4 compression 35M/6.6M ~ 5.3x 3) LZ4HC compression
 * 35M/6.1M ~ 5.7x
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 1.08/0.84 = 1.29 (to no compression) LZ4HC - 1.17/0.84 = 1.4 (to no compression)
 *
 * <p>Notes: Redis can store these URLS in a Hash with ziplist encoding using HSET ( key, field =
 * URL, "1")
 *
 * <p>Avg. URL size is 14. ziplist overhead for {URL, "1"} pair is 4 = 2+2. So usage is going to be
 * 18 per URL
 *
 * <p>LZ4HC compression = 12 bytes per URL. So, we have at least 1.5x advantage even for this Redis
 * hack
 *
 * <p>Notes: Using Hash hack to store large set of objects has one downside - you can't use SET
 * specific API: union, intersect etc.
 *
 * <p>************************ Hashes HSET hashed_key host "1"
 *
 * <p>Confirmed: Redis consumes 19 bytes per host in a highly optimized representation of a large
 * set using multiple hashes with ziplist encoding
 *
 * <p>Total 10MB size for 516720 hosts
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 10M/8.5M ~ 1.18x 2) LZ4 compression 10M/6.6M ~ 1.5x 3) LZ4HC compression
 * 10M/6.1M ~ 1.65x
 */
public class RedisHashesSpamFilter {

  private static final Logger log = LogManager.getLogger(RedisHashesSpamFilter.class);

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      usage();
    }
    log.debug("RUN Redis test");
    runTest(args[0]);
  }

  @SuppressWarnings("deprecation")
  private static void runTest(String fileName) throws IOException {

    Jedis client = new Jedis("localhost");

    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String line = null;
    long totalLength = 0;
    int count = 0;

    String baseKey = "key";

    long startTime = System.currentTimeMillis();
    while ((line = dis.readLine()) != null) {
      totalLength += line.length();
      count++;
      int hash = Math.abs(line.hashCode());
      int rem = hash % 100;
      String key = baseKey + rem;
      client.hset(key, line, "1");
      if ((count % 10000) == 0 && count > 0) {
        log.debug("Loaded {}", count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} records, total size={} in {}ms.", count, totalLength, endTime - startTime);
    dis.close();

    log.debug("Press any key ...");
    System.in.read();

    for (int i = 0; i < 100; i++) {
      String key = baseKey + i;
      client.del(key);
    }

    client.close();
  }

  private static void usage() {
    log.fatal("usage: java org.bigbase.carrot.examples.SpamFilter domain_list_file");
    System.exit(-1);
  }
}
