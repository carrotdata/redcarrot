/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.examples.appcomps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.util.Bytes;

/**
 * Inverted index implemented accordingly to Redis Book:
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-7-search-based-applications/7-1-searching-in-redis/7-1-1-basic-search-theory/
 * <p>
 * We emulate 1000 words and some set of docs. Maximum occurrence of a single word is 5000 docs
 * (random number between 1 and 5000). Each doc is coded by 4-byte integer. Each word is a random 8
 * byte string.
 * <p>
 * Format of an inverted index:
 * <p>
 * word -> {id1, id2, ..idk}, idn - 4 - byte integer
 * <p>
 * Redis takes 64.5 bytes per one doc id (which is 4 byte long) Carrot takes 5.8 bytes
 * <p>
 * Redis - to - Carrot memory usage = 64.5/5.8 = 11.1 Because the data is poorly compressible we
 * tested only Carrot w/o compression.
 */
import redis.clients.jedis.Jedis;

public class TestRedisInvertedIndex {

  private static final Logger log = LogManager.getLogger(TestRedisInvertedIndex.class);

  static int numWords = 1000;
  static int maxDocs = 5000;

  public static void main(String[] args) throws IOException {
    runTest();
  }

  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");

    Random r = new Random();
    long totalSize = 0;
    List<byte[]> keys = new ArrayList<byte[]>();

    long start = System.currentTimeMillis();
    for (int i = 0; i < numWords; i++) {
      // all words are size of 4;
      byte[] key = new byte[4];
      r.nextBytes(key);
      keys.add(key);
      int max = r.nextInt(maxDocs) + 1;
      for (int j = 0; j < max; j++) {
        int v = r.nextInt();
        byte[] value = Bytes.toBytes(v);
        client.sadd(key, value);
        totalSize++;
      }
      if (i % 100 == 0) {
        log.debug("Loaded {}", i);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("Loaded {} in {}ms. Press any button ...", totalSize, end - start);
    System.in.read();
    for (byte[] k : keys) {
      client.del(k);
    }
    client.close();
  }
}
