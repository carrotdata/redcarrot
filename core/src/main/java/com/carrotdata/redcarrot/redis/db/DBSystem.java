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
package com.carrotdata.redcarrot.redis.db;

import static com.carrotdata.redcarrot.redis.util.Commons.KEY_SIZE;

import java.util.concurrent.atomic.AtomicLong;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.util.DataType;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class DBSystem {

  /** This MUST be configurable */
  private static long DEFAULT_CURSOR_EXPIRATION = 3600000; // in ms

  private static AtomicLong seqId = new AtomicLong(0);
  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };

  private static ThreadLocal<Integer> keyArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };

  /**
   * Checks key arena size
   * @param required size
   */
  static void checkKeyArena(int required) {
    int size = keyArenaSize.get();
    if (size >= required) {
      return;
    }
    long ptr = UnsafeAccess.realloc(keyArena.get(), required);
    keyArena.set(ptr);
    keyArenaSize.set(required);
  }

  /**
   * Build key for System. It uses thread local key arena TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @return address of a system key
   */
  public static long buildKey(long keyPtr, int keySize) {
    checkKeyArena(keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    UnsafeAccess.putByte(arena, (byte) DataType.SYSTEM.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    return arena;
  }

  /**
   * Get system key size
   * @param keySize
   * @return system key size
   */
  public static int getSystemKeySize(int keySize) {
    return keySize + KEY_SIZE + Utils.SIZEOF_BYTE;
  }

  /**
   * Build key for System. It uses provided arena
   * @param keyPtr original key address
   * @param keySize original key size
   * @return new key size
   */
  public static int buildKey(long keyPtr, int keySize, long arena) {
    UnsafeAccess.putByte(arena, (byte) DataType.SYSTEM.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    return keySize + KEY_SIZE + Utils.SIZEOF_BYTE;
  }

  /**
   * Returns next id
   * @return id
   */
  public static long nextId() {
    // Do not return 0
    return seqId.incrementAndGet();
  }

  /** Use for testing */
  public static void reset() {
    seqId.set(0);
  }

  /**
   * Saves cursor id with associated value (last seen key)
   * @param map sorted map storage
   * @param cursorId cursor id
   * @param valPtr value address
   * @param valSize value size
   * @return true on success, false - otherwise
   */
  public static boolean saveCursor(BigSortedMap map, long cursorId, long valPtr, int valSize) {
    long ptr = UnsafeAccess.malloc(getSystemKeySize(Utils.SIZEOF_LONG));
    UnsafeAccess.putLong(ptr, cursorId);
    long keyPtr = buildKey(ptr, Utils.SIZEOF_LONG);
    int keySize = getSystemKeySize(Utils.SIZEOF_LONG);
    boolean result = map.put(keyPtr, keySize, valPtr, valSize,
      DEFAULT_CURSOR_EXPIRATION + java.lang.System.currentTimeMillis());
    UnsafeAccess.free(ptr);
    return result;
  }

  /**
   * Get last seen key by a given cursor
   * @param map sorted map storage
   * @param cursorId cursor id
   * @param bufferPtr buffer address
   * @param bufferSize buffer size
   * @return size of a value
   */
  public static int getCursor(BigSortedMap map, long cursorId, long bufferPtr, int bufferSize) {
    long ptr = UnsafeAccess.malloc(getSystemKeySize(Utils.SIZEOF_LONG));
    UnsafeAccess.putLong(ptr, cursorId);
    long keyPtr = buildKey(ptr, Utils.SIZEOF_LONG);
    int keySize = getSystemKeySize(Utils.SIZEOF_LONG);
    int size = (int) map.get(keyPtr, keySize, bufferPtr, bufferSize, 0);
    UnsafeAccess.free(ptr);
    return size;
  }

  /**
   * Deletes obsolete cursor
   * @param map sorted map storage
   * @param cursorId cursor id
   * @return true on success, false - otherwise
   */
  public static boolean deleteCursor(BigSortedMap map, long cursorId) {
    long ptr = UnsafeAccess.malloc(getSystemKeySize(Utils.SIZEOF_LONG));
    UnsafeAccess.putLong(ptr, cursorId);
    long keyPtr = buildKey(ptr, Utils.SIZEOF_LONG);
    int keySize = getSystemKeySize(Utils.SIZEOF_LONG);
    boolean result = map.delete(keyPtr, keySize);
    UnsafeAccess.free(ptr);
    return result;
  }
}
