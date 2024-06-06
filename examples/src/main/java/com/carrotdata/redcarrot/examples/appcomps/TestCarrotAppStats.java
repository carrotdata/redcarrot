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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.redis.RedisConf;

/**
 * Counters and statistics. Redis Book, Chapter 5.1:
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-5-using-redis-for-application-support/
 * 5-2-counters-and-statistics/5-2-2-storing-statistics-in-redis/
 * <p>
 * We implement simple application which stores Web-application's page access time statistics. The
 * app is described in the Redis book (see the link above)
 * <p>
 * We collect hourly statistics for 1 year on web page access time
 * <p>
 * The key = "stats:profilepage:access:hour"
 * <p>
 * The key consists from several parts:
 * <p>
 * stats - This is top group name - means "Statistics" profilepage - page we colect statistics on
 * access - statistics on total access time hour - 8 byte timestamp for the hour
 * <p>
 * There are 24*365 = 8,760 hours in a year, so there are 8,760 keys in the application. For Redis
 * we will use Hash as recommended in the book), for Carrot we will use HASH as well type to store
 * the data.
 * <p>
 * We collect the following statistics:
 * <p>
 * "min" - minimum access time "max" - maximum access time "count" - total number of accesses "sum"
 * - sum of access times "sumsq" - sum of squares of access time
 * <p>
 * The above info information will allow us to calculate std deviation, min, max, average, total.
 * <p>
 * Results Carrot: 1. No compression - 11,644,928 2. LZ4 - 5,789,568 3. ZSTD - 3,873,088
 * <p>
 * Results Redis ~ 16,400,000
 * <p>
 * Redis/Carrot memory usage:
 * <p>
 * 1. No compression = 1.45 2. LZ4 compression = 3.0 3. ZSTD compression = 4.2
 */
public class TestCarrotAppStats {

  private static final Logger log = LogManager.getLogger(TestCarrotAppStats.class);

  static final String KEY_PREFIX = "stats:profilepage:access:";
  static final int hoursToKeep = 10 * 365 * 24;

  public static void main(String[] args) {
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
    runTestNoCompression();
    runTestCompressionLZ4();
    runTestCompressionZSTD();
  }

  private static void runTestNoCompression() {
    log.debug("\nTest , compression = None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
  }

  private static void runTestCompressionLZ4() {
    log.debug("\nTest , compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
  }

  private static void runTestCompressionZSTD() {
    log.debug("\nTest , compression = ZSTD");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    runTest();
  }

  private static void runTest() {
    BigSortedMap map = new BigSortedMap(100000000);
    long start = System.currentTimeMillis();
    for (int i = 0; i < hoursToKeep; i++) {
      Stats st = Stats.newStats(i);
      st.saveToCarrotNative(map);
    }
    long end = System.currentTimeMillis();
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    log.debug("Loaded {} in {}ms RAM usage={} bytes", hoursToKeep, end - start, memory);
    map.dispose();
  }
}
