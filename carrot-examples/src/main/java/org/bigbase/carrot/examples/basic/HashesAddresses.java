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

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.examples.util.Address;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.KeyValue;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example shows how to use Carrot Hashes to store user Address objects This example
 * demonstrate how compression works with a real data set Data set is the random sample of a
 * openaddress.org data set for US-WEST region
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
 * <p>Redis estimate per address object, using Hashes with ziplist encodings (most efficient) is 161
 * (actually it can be more, this is a low estimate based on evaluation of a Redis code)
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 161/124 = 1.3x 2) LZ4 compression 161/66 = 2.44x 3) LZ4HC compression
 * 161/63.6 = 2.53x
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 1.3/0.7 = 1.86x (to no compression) LZ4HC - 1.35/0.7 = 1.93x (to no compression)
 */
public class HashesAddresses {

  private static final Logger log = LogManager.getLogger(HashesAddresses.class);

  static {
    UnsafeAccess.debug = true;
  }

  static long keyBuf = UnsafeAccess.malloc(64);
  static long fieldBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(64);

  static long N = 100000;
  static long totalDataSize = 0;
  static List<Address> addressList;

  public static void main(String[] args) throws IOException, OperationFailedException {

    addressList = Address.loadFromFile(args[0]);

    log.debug("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
    log.debug("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
    log.debug("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();
  }

  private static void runTest() throws IOException, OperationFailedException {

    BigSortedMap map = new BigSortedMap(1000000000);

    totalDataSize = 0;

    long startTime = System.currentTimeMillis();
    int count = 0;

    for (Address us : addressList) {
      count++;
      String skey = Address.getUserId(count);
      byte[] bkey = skey.getBytes();
      int keySize = bkey.length;
      UnsafeAccess.copy(bkey, 0, keyBuf, keySize);

      totalDataSize += keySize;

      List<KeyValue> list = us.asList();
      totalDataSize += Utils.size(list);

      int num = Hashes.HSET(map, keyBuf, keySize, list);
      if (num != list.size()) {
        log.fatal("ERROR in HSET");
        System.exit(-1);
      }

      if (count % 10000 == 0) {
        log.debug("set {}", count);
      }

      list.forEach(KeyValue::free);
    }
    long endTime = System.currentTimeMillis();

    log.debug(
        "Loaded {} user address objects, total size={} in {}ms. RAM usage={} COMPRESSION={}",
        addressList.size(),
        totalDataSize,
        endTime - startTime,
        UnsafeAccess.getAllocatedMemory(),
        (double) totalDataSize / UnsafeAccess.getAllocatedMemory());

    BigSortedMap.printGlobalMemoryAllocationStats();

    map.dispose();
  }
}
