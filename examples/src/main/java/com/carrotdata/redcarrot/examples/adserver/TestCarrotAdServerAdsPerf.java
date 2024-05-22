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
package com.carrotdata.redcarrot.examples.adserver;

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.hashes.Hashes;
import com.carrotdata.redcarrot.redis.zsets.ZSets;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

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
 * <p>
 * Redis 7.2.4 = 1,527,734,024 
 * Carrot no compression = 652,325,376 
 * Carrot LZ4 compression = 476,455,424 
 * Carrot ZSTD compression = 364,447,872
 *
 *  Memory ratio Redis/Carrot:
 *  
 *   1. Carrot no compression ~ 2.34x
 *   2. Carrot LZ4            ~ 3.2x
 *   3. Carrot ZSTD           ~ 4.2x  
 * <p>Notes:
 *
 * <p>1. The test uses synthetic data, which is mostly random and not compressible 2. For AdSitePerf
 * dataset we use compact hashes in Redis to minimize memory usage.
 */
public class TestCarrotAdServerAdsPerf {

  private static final Logger log = LogManager.getLogger(TestCarrotAdServerAdsPerf.class);

  static final int MAX_ADS = 1000;
  static final int MAX_SITES = 10000;

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
    BigSortedMap map = new BigSortedMap(10000000000L);
    doAdsSitePerf(map);
    doAdsSiteRank(map);
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    log.debug("Total memory={}", memory);
    map.dispose();
  }

  private static void doAdsSiteRank(BigSortedMap map) {
    log.debug("Loading AdsSiteRank data");
    String key = "ads:sites:rank";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_SITES; j++) {
        int siteId = j;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr, siteId);
        ZSets.ZADD(
            map,
            keyPtr,
            keySize,
            new double[] {r.nextDouble()},
            new long[] {mPtr},
            new int[] {mSize},
            true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          log.debug("AdsSiteRank :{}", count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    log.debug("AdsSiteRank : loaded {} in {}ms", count, end - start);
  }

  private static void doAdsSitePerf(BigSortedMap map) {
    log.debug("Loading Ads-Site Performance data");
    String key = "ads:site:perf:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_ADS; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_SITES; j++) {
        count++;
        long[] mPtrs =
            new long[] {
              UnsafeAccess.malloc(Utils.SIZEOF_INT + 1), UnsafeAccess.malloc(Utils.SIZEOF_INT + 1)
            };

        int[] mSizes = new int[] {Utils.SIZEOF_INT + 1, Utils.SIZEOF_INT + 1};
        UnsafeAccess.putInt(mPtrs[0], MAX_ADS - j);
        UnsafeAccess.putByte(mPtrs[0] + Utils.SIZEOF_INT, (byte) 0);
        UnsafeAccess.putInt(mPtrs[1], MAX_ADS - j);
        UnsafeAccess.putByte(mPtrs[1] + Utils.SIZEOF_INT, (byte) 1);

        long[] vPtrs =
            new long[] {
              UnsafeAccess.malloc(Utils.SIZEOF_INT), UnsafeAccess.malloc(Utils.SIZEOF_INT)
            };
        int[] vSizes = new int[] {Utils.SIZEOF_INT, Utils.SIZEOF_INT};
        UnsafeAccess.putInt(vPtrs[0], r.nextInt(100));
        UnsafeAccess.putInt(vPtrs[1], r.nextInt(100));

        Hashes.HSET(map, keyPtr, keySize, mPtrs, mSizes, vPtrs, vSizes);
        UnsafeAccess.free(vPtrs[0]);
        UnsafeAccess.free(vPtrs[1]);
        UnsafeAccess.free(mPtrs[0]);
        UnsafeAccess.free(mPtrs[1]);

        if (count % 100000 == 0) {
          log.debug("AdsSitePerf :{}", count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }

    long end = System.currentTimeMillis();
    log.debug("AdsSitePerf : loaded {} in {}ms", count, end - start);
  }
}
