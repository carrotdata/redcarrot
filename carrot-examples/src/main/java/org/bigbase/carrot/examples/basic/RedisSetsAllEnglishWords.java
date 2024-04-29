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
 * This example shows how to use Redis Set to keep list of all English words
 *
 * <p>File: words_alpha.txt.s.
 *
 * <p>RESULTS: 0. Total number of words is 370099 1. Raw size of all words is 3,494,665 bytes 2.
 * Carrot NoCompression - RAM usage 4,306,191, COMPRESSION = 0.81 3 Carrot LZ4 compression - RAM
 * usage 2,857,311, COMPRESSION = 1.22 4. Carrot LZ4HC compression - RAM usage 2,601,695,
 * COMPRESSION = 1.34
 *
 * <p>LZ4 compression relative to NoCompression = 1.22/0.81 = 1.5 LZ4HC compression relative to
 * NoCompression = 1.34/0.81 = 1.65
 *
 * <p>Redis SET RAM usage is 24.3MB (~ 66 bytes per word)
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 24.3M/4.3M ~ 5.7x 2) LZ4 compression 24.3M/2.8M ~ 8.4x 3) LZ4HC compression
 * 24.3M/2.6M ~ 9.0x
 */
public class RedisSetsAllEnglishWords {

  private static final Logger log = LogManager.getLogger(RedisSetsAllEnglishWords.class);

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

    String key = "key";

    long startTime = System.currentTimeMillis();
    while ((line = dis.readLine()) != null) {
      totalLength += line.length();
      count++;
      client.sadd(key, line);
      if ((count % 10000) == 0 && count > 0) {
        log.debug("Loaded {}", count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} records, total size={} in {}ms.", count, totalLength, endTime - startTime);
    dis.close();

    log.debug("Press any key ...");
    System.in.read();
    client.del(key);
    client.close();
  }

  private static void usage() {
    log.fatal("usage: java org.bigbase.carrot.examples.RedisSetsAllEnglishWords word_list_file");
    System.exit(-1);
  }
}
