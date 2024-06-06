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
package com.carrotdata.redcarrot.examples.geoip;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * Test Carrot GeoIp GeoIP application as described in Redis Book chapter 5.3 "IP-to-city and
 * country lookup":
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-5-using-redis-for-application-support/5-3-ip-to-city-and-country-lookup/
 * <p>
 * We implemented application in both: Carrot and Redis.
 * <p>
 * Carrot implementation details:
 * <p>
 * Carrot uses SET data type to store combined NetworkAddress, City ID pair. Carrot's SETs are
 * ordered, so they can be used to answer the following questions:
 * <p>
 * Give me the greatest member which is less or equals to a given search key, therefore it can be
 * used to locate network which a given IP address belongs to.
 * <p>
 * Carrot uses STRING data type (plain key-value) to keep association between CityID and City
 * location, name and other data.
 * <p>
 * key = CityId value = {comma separated string of a city data}
 * <p>
 * We used Ip-Geo database free version from www.maxmind.com
 * <p>
 * Redis implementation details:
 * <p>
 * Redis uses ZSET (ordered set) to keep NetworkAddress -> city ID association.
 * <p>
 * Memory usage:
 * <p>
 * 1. No compression - 79,707,904 2. LZ4 compression - 38,663,616 3. ZSTD compression - 32,765,568
 * <p>
 * Redis usage - 406,372,784
 * <p>
 * Redis/Carrot:
 * <p>
 * No compression 406/80 = 5.1 LZ4 406/38 = 10.7 ZSTD 406/33 = 12.3
 */
public class TestCarrotGeoIP {

  private static final Logger log = LogManager.getLogger(TestCarrotGeoIP.class);

  static List<CityBlock> blockList;
  static List<CityLocation> locList;

  public static void main(String[] args) throws IOException {
    runNoCompression(args[0], args[1]);
    runCompressionLZ4(args[0], args[1]);
    runCompressionZSTD(args[0], args[1]);
  }

  private static void runNoCompression(String f1, String f2) throws IOException {
    log.debug("Compression=NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest(f1, f2);
  }

  private static void runCompressionLZ4(String f1, String f2) throws IOException {
    log.debug("Compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest(f1, f2);
  }

  private static void runCompressionZSTD(String f1, String f2) throws IOException {
    log.debug("Compression=ZSTD");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    runTest(f1, f2);
  }

  private static void runTest(String f1, String f2) throws IOException {
    BigSortedMap map = new BigSortedMap(1000000000);
    if (blockList == null) {
      blockList = CityBlock.load(f1);
    }
    long ptr = UnsafeAccess.allocAndCopy("key1", 0, "key1".length());
    int size = "key1".length();
    long start = System.currentTimeMillis();
    int total = 0;
    for (CityBlock cb : blockList) {
      cb.saveToCarrot(map, ptr, size);
      total++;
      if (total % 100000 == 0) {
        log.debug("Total blocks={}", total);
      }
    }
    long end = System.currentTimeMillis();

    log.debug("Loaded {} blocks in {}ms", blockList.size(), end - start);
    total = 0;
    if (locList == null) {
      locList = CityLocation.load(f2);
    }
    start = System.currentTimeMillis();
    for (CityLocation cl : locList) {
      cl.saveToCarrot(map);
      total++;
      if (total % 100000 == 0) {
        log.debug("Total locs={}", total);
      }
    }
    end = System.currentTimeMillis();

    log.debug("Loaded {} locations in {}ms", locList.size(), end - start);
    log.debug("Total memory used={}", BigSortedMap.getGlobalAllocatedMemory());

    map.dispose();
  }
}
