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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.KeyValue;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example shows how to use Carrot Hashes to store user session objects
 *
 * <p>User Session structure: "SessionID" - A unique, universal identifier for the session data
 * structure (16 bytes). "Host" - host name or IP Address The location from which the client
 * (browser) is making the request. "UserID" - Set to the user's distinguished name (DN) or the
 * application's principal name. "Type" - USER or APPLICATION "State" - session state: VALID,
 * INVALID Defines whether the session is valid or invalid. "MaxIdleTime" - Maximum Idle Time
 * Maximum number of minutes without activity before the session will expire and the user must
 * reauthenticate. "MaxSessionTime" - Maximum Session Time. Maximum number of minutes (activity or
 * no activity) before the session expires and the user must reauthenticate. "MaxCachingTime" -
 * Maximum number of minutes before the client contacts OpenSSO Enterprise to refresh cached session
 * information. "StartTime" - session start time (seconds since 01.01.1970) "LastActiveTime" - last
 * interaction time (seconds since 01.01.1970) userId is used as a KEY for Carrot and Redis Hash
 *
 * <p>Test description: <br>
 * UserSession object has 10 fields, one field (UserId) is used as a Hash key
 *
 * <p>Average key + session object size is 192 bytes. We load 100K user session objects
 *
 * <p>Results: 0. Average user session data size = 192 bytes 1. No compression. Used RAM per session
 * object is 249 bytes (COMPRESSION= 0.77) 2. LZ4 compression. Used RAM per session object is 90
 * bytes (COMPRESSION = 2.14) 3. LZ4HC compression. Used RAM per session object is 87 bytes
 * (COMPRESSION = 2.20)
 *
 * <p>Redis estimate per session object, using Hashes with ziplist encodings is 290 (actually it can
 * be more, this is a low estimate based on evaluating Redis code)
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 290/249 = 1.17x 2) LZ4 compression 290/90 = 3.22x 3) LZ4HC compression
 * 290/87 = 3.33x
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 3.22/1.17 = 2.75x (to no compression) LZ4HC - 3.33/1.17 = 2.85x (to no compression)
 */
public class HashesUserSessions {

  private static final Logger log = LogManager.getLogger(HashesUserSessions.class);

  static {
    UnsafeAccess.debug = true;
  }

  static long keyBuf = UnsafeAccess.malloc(64);
  static long fieldBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(64);

  static long N = 100000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();

  static {
    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
  }

  public static void main(String[] args) throws IOException, OperationFailedException {

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

    for (UserSession us : userSessions) {
      count++;
      String skey = us.getUserId();
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
        "Loaded {} user sessions, total size={} in {}ms. RAM usage={} COMPRESSION={}",
        userSessions.size(),
        totalDataSize,
        endTime - startTime,
        UnsafeAccess.getAllocatedMemory(),
        (double) totalDataSize / UnsafeAccess.getAllocatedMemory());

    BigSortedMap.printGlobalMemoryAllocationStats();
    map.dispose();
  }
}
