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
import com.carrotdata.redcarrot.examples.util.UserSession;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.util.UnsafeAccess;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Carrot Hashes to store user session objects
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
 * information. "StartTime" - session start time (seconds since 01.01.1970) "LastActiveTime" - last
 * interaction time (seconds since 01.01.1970) userId is used as a KEY for Carrot and Redis Hash
 * <p>
 * Test description: <br>
 * UserSession object has 10 fields, one field (UserId) is used as a Hash key
 * <p>
 * Average key + session object size is 234 bytes. We load 1M user session objects
 * <p>
 * Results: 0. Average user session data size = 234 bytes 1. No compression. Used RAM per session
 * object is 284 bytes (COMPRESSION= 0.82) 2. LZ4 compression. Used RAM per session object is 111
 * bytes (COMPRESSION = 2.11) 3. ZSTD compression. Used RAM per session object is 64
 * bytes(COMPRESSION = 3.66)
 * <p>
 * Redis estimate per session object, using Hashes with ziplist encodings is 290 (actually it can be
 * more, this is a low estimate based on evaluating Redis code)
 * <p>
 * RAM usage (Redis-to-Carrot)
 * <p>
 * 1) No compression 290/284 ~ 1.x 2) LZ4 compression 290/111 = 2.6x 3) ZSTD compression 290/64 =
 * 4.5x
 * <p>
 * Effect of a compression:
 * <p>
 * LZ4 - 2.11/0.82 = 2.57x (to no compression) ZSTD - 3.66/0.82 = 4.46x (to no compression)
 */
public class RedisUserSessions {

  private static final Logger log = LogManager.getLogger(HashesUserSessions.class);

  static {
    // UnsafeAccess.setMallocDebugEnabled(true);
  }

  static long keyBuf = UnsafeAccess.malloc(64);
  static long fieldBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(64);

  static long N = 1000000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();

  public static void main(String[] args) throws IOException, OperationFailedException {
    createSessions();
    runTest();
  }

  private static void createSessions() {
    log.debug("Creating sessions ...");

    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
    log.debug("Creating sessions finished");

  }

  private static void runTest() throws IOException, OperationFailedException {
    log.debug("Running test");
    Jedis client = new Jedis("localhost");
    totalDataSize = 0;

    long startTime = System.currentTimeMillis();
    int count = 0;

    for (UserSession us : userSessions) {
      count++;
      us.saveToRedisNative(client);
      if (count % 10000 == 0) {
        log.info("set {}", count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} user sessions, total size={} in {}ms.", userSessions.size(),
      endTime - startTime);
  }
}
