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
package com.carrotdata.redcarrot.redis;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.commands.RedisCommand;
import com.carrotdata.redcarrot.redis.commands.SHUTDOWN;
import com.carrotdata.redcarrot.redis.util.Utils;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;

public class CommandProcessor {

  private static final Logger log = LogManager.getLogger(CommandProcessor.class);

  /*
   * Default memory buffer size for IO operations
   */
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB

  /** Keeps thread local Key instance */
  private static ThreadLocal<Key> keyTLS = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0, 0);
    }
  };

  /** Input buffer per thread TODO: floating size */
  private static ThreadLocal<Long> inBufTLS = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      long ptr = UnsafeAccess.malloc(BUFFER_SIZE);
      return ptr;
    }
  };

  /*
   * Output buffer per thread TODO: floating size
   */
  private static ThreadLocal<Long> outBufTLS = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      long ptr = UnsafeAccess.malloc(BUFFER_SIZE);
      return ptr;
    }
  };

  /*
   * Redis command map.
   */
  private static ThreadLocal<HashMap<Key, RedisCommand>> commandMapTLS =
      new ThreadLocal<HashMap<Key, RedisCommand>>() {
        @Override
        protected HashMap<Key, RedisCommand> initialValue() {
          return new HashMap<Key, RedisCommand>();
        }
      };

  private static final byte[] WRONG_REQUEST_FORMAT = "-ERR: Wrong request format".getBytes();
  private static final byte[] UNSUPPORTED_COMMAND = "-ERR: Unsupported command: ".getBytes();

  /**
   * Main method
   * @param in input buffer contains incoming Redis command
   * @param out output buffer to return to a client (command response)
   * @return true , if shutdown was requested, false - otherwise
   */
  static long executeTotal = 0;

  static int count = 0;

  @SuppressWarnings("deprecation")
  public static boolean process(BigSortedMap storage, ByteBuffer in, ByteBuffer out) {
    count++;
    long inbuf = inBufTLS.get();
    // Convert Redis request to a Carrot internal format
    boolean result = Utils.requestToCarrot(in, inbuf, BUFFER_SIZE);

    if (!result) {
      out.put(WRONG_REQUEST_FORMAT);
      return false;
    }
    HashMap<Key, RedisCommand> map = commandMapTLS.get();
    Key key = getCommandKey(inbuf);
    RedisCommand cmd = map.get(key);
    if (cmd == null) {
      String cmdName = com.carrotdata.redcarrot.util.Utils.toString(key.address, key.length);
      try {
        @SuppressWarnings("unchecked")
        Class<RedisCommand> cls = (Class<RedisCommand>) Class
            .forName("com.carrotdata.redcarrot.redis.commands." + cmdName);
        cmd = cls.newInstance();
        map.put(key, cmd);
      } catch (Throwable e) {
        out.put(UNSUPPORTED_COMMAND);
        out.put(cmdName.getBytes());
        out.put((byte) '\r');
        out.put((byte) '\n');
        return false;
      }
    }
    long outbuf = outBufTLS.get();
    // Execute Redis command
    long start = System.nanoTime();
    cmd.executeCommand(storage, inbuf, outbuf, BUFFER_SIZE);
    executeTotal += System.nanoTime() - start;
    if (count % 10000 == 0) {
      // log.debug(" command exe avg={}", executeTotal / (1000L * count));
    }
    if (cmd.autoconvertToRedis()) {
      // Convert response to Redis format
      Utils.carrotToRedisResponse(outbuf, out);
    } else {
      // Let command implement custom conversion
      cmd.convertToRedis(out);
    }
    // Done.
    return cmd instanceof SHUTDOWN;
  }

  /**
   * Extract command name from an input buffer
   * @param inbuf input buffer
   * @return command name as a Key
   */
  private static Key getCommandKey(long inbuf) {
    Key key = keyTLS.get();
    int cmdLen = UnsafeAccess.toInt(inbuf + com.carrotdata.redcarrot.util.Utils.SIZEOF_INT);
    key.address = inbuf + 2 * com.carrotdata.redcarrot.util.Utils.SIZEOF_INT;
    key.length = cmdLen;
    // To upper case
    com.carrotdata.redcarrot.util.Utils.toUpperCase(key.address, key.length);
    return key;
  }
}
