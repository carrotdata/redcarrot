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

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * ----- Data structures to keep site performance
 *
 * <p>13. SiteAdsRank: ZSET - ordered set. key = siteId, member = adId, score = CTR (click through
 * rate). This data allows us to estimate how does the ad perform on a particular site relative to
 * other ads.
 *
 * <p>14. SiteWordsRank: ZSET - ordered set. key = siteId, member = word, score - word's value. This
 * data store keeps keywords with corresponding scores. Every time someone acts on ads on the site,
 * all keywords from the ad are added to the site's ordered set with a some score value. The more a
 * keyword appears in the ads - the higher it is going to be in the site's list. This data allows us
 * to estimate the most important keywords for the site as well as targeting attributes.
 *
 * <p>Results:
 *
 * <p>
 * 
 * 1. Redis 7.2.4 = 2,421,441,296 
 * 2. Carrot no compression = 733,330,944 
 * 3. Carrot LZ4 compression = 675,045,888 
 * 4. Carrot ZSTD compression = 577,178,368
 * 
 *    Memory ratio Redis/Carrot:
 *  
 *   1. Carrot no compression ~ 3.3x
 *   2. Carrot LZ4            ~ 3.6x
 *   3. Carrot ZSTD           ~ 4.2x  
 *
 * <p>Notes:
 *
 * <p>1. The test uses synthetic data, which is mostly random and not compressible
 */
public class TestCarrotAdServerSitePerf {

  private static final Logger log = LogManager.getLogger(TestCarrotAdServerSitePerf.class);

  static final int MAX_ADS = 10000;
  static final int MAX_WORDS = 10000;
  static final int MAX_SITES = 1000;

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
    doSiteAdsRank(map);
    doSiteWordsRank(map);
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    log.debug("Total memory={}", memory);
    map.dispose();
  }

  private static void doSiteWordsRank(BigSortedMap map) {
    log.debug("Loading SiteWordsRank data");
    String key = "sites:words:rank:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_SITES; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_WORDS; j++) {
        String word = Utils.getRandomStr(r, 8);
        long mPtr = UnsafeAccess.allocAndCopy(word, 0, word.length());
        int mSize = word.length();
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
          log.debug("SiteWordsRank :{}", count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    log.debug("SiteWordsRank : loaded {} in {}ms", count, end - start);
  }

  private static void doSiteAdsRank(BigSortedMap map) {
    log.debug("Loading  SiteAdsRank data");
    String key = "sites:ads:rank:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_SITES; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_ADS; j++) {
        int adsId = j;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr, adsId);
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
          log.debug("SiteAdsRank :{}", count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    log.debug("SiteAdsRank : loaded {} in {}ms", count, end - start);
  }
}
