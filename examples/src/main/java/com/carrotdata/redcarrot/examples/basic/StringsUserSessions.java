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
package com.carrotdata.redcarrot.examples.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.examples.util.UserSession;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.strings.Strings;
import com.carrotdata.redcarrot.redis.util.MutationOptions;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Strings to store user sessions objects
 * <p>
 * User Session structure: "SessionID" - A unique, universal identifier for the session data
 * structure (16 bytes). "Host" - host name or IP Address The location from which the client
 * (browser) is making the request. "UserID" - Set to the user's distinguished name (DN) or the
 * application's principal name. "Type" - USER or APPLICATION "State" - session state: VALID,
 * INVALID Defines whether the session is valid or invalid. "MaxIdleTime" - Maximum Idle Time
 * Maximum number of minutes without activity before the session will expire and the user must
 * reauthenticate. "MaxSessionTime" - Maximum Session Time. Maximum number of minutes (activity or
 * no activity) before the session expires and the user must reauthenticate. "MaxCachingTime" -
 * Maximum number of minutes before the client contacts OpenSSO Enterprise to refresh cached session
 * information.
 * <p>
 * Test description: <br>
 * UserSession object has 8 fields, one field (UserId) is used as a String key
 * <p>
 * Average key + session object size is 222 bytes. We load 100K user session objects
 * <p>
 * Results: 0. Average user session data size = 222 bytes (includes key size) 1. No compression.
 * Used RAM per session object is 300 bytes (COMPRESSION= 0.8) 2. LZ4 compression. Used RAM per
 * session object is 115 bytes (COMPRESSION = 2.37) 3. LZ4HC compression. Used RAM per session
 * object is 108 bytes (COMPRESSION = 2.5), ZSTD - 65 bytes
 * <p>
 * Redis using String encoding is ~314 bytes per session
 * <p>
 * RAM usage (Redis-to-Carrot)
 * <p>
 * 1) No compression 314/300 ~ 1.07x 2) LZ4 compression 314/115 ~ 2.8x 3) LZ4HC compression 314/108
 * ~ 2.96x, ZSTD compression 314/65 ~ 4.8x
 * <p>
 * Effect of a compression:
 * <p>
 * LZ4 - 2.37/0.8 = 2.96 (to no compression) LZ4HC - 2.5/0.8 = 3.13 (to no compression)
 */
public class StringsUserSessions {

  private static final Logger log = LogManager.getLogger(StringsUserSessions.class);

  static {
    // UnsafeAccess.debug = true;
  }

  static long keyBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(512);
  static long N = 1000000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();

  static {
    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
  }

  public static void main(String[] args) throws IOException, OperationFailedException {
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
    log.debug("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
    log.debug("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
    log.debug("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();
    log.debug("RUN compression = ZSTD");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
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
      String svalue = us.toString();
      byte[] bvalue = svalue.getBytes();

      totalDataSize += bkey.length + bvalue.length;

      UnsafeAccess.copy(bkey, 0, keyBuf, bkey.length);
      UnsafeAccess.copy(bvalue, 0, valBuf, bvalue.length);

      boolean result = Strings.SET(map, keyBuf, bkey.length, valBuf, bvalue.length, 0,
        MutationOptions.NONE, true);
      if (!result) {
        log.fatal("ERROR in SET");
        System.exit(-1);
      }

      if (count % 10000 == 0) {
        log.debug("set {}", count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} user sessions, total size={} in {}ms. RAM usage={} COMPRESSION={}",
      userSessions.size(), totalDataSize, endTime - startTime, UnsafeAccess.getAllocatedMemory(),
      (double) totalDataSize / UnsafeAccess.getAllocatedMemory());

    BigSortedMap.printGlobalMemoryAllocationStats();
    map.dispose();
  }
}
