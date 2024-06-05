/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.strings;

import static com.carrotdata.redcarrot.redis.util.Commons.KEY_SIZE;
import static com.carrotdata.redcarrot.util.KeysLocker.writeLock;
import static com.carrotdata.redcarrot.util.KeysLocker.writeUnlock;

import java.util.List;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.redis.util.BitOp;
import com.carrotdata.redcarrot.redis.util.DataType;
import com.carrotdata.redcarrot.redis.util.MutationOptions;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.KeyValue;
import com.carrotdata.redcarrot.util.KeysLocker;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

/** Supports String operations, found in Redis */
public class Strings {

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

  static ThreadLocal<Long> valueArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };

  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };

  static final int INCR_ARENA_SIZE = 64;

  static ThreadLocal<Long> incrArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(INCR_ARENA_SIZE);
    }
  };

  static ThreadLocal<Key> key = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0, 0);
    }
  };

  /** Thread local updates String Append */
  private static ThreadLocal<StringAppend> stringAppend = new ThreadLocal<StringAppend>() {
    @Override
    protected StringAppend initialValue() {
      return new StringAppend();
    }
  };

  /** Thread local updates String Bitcount */
  private static ThreadLocal<StringBitCount> stringBitcount = new ThreadLocal<StringBitCount>() {
    @Override
    protected StringBitCount initialValue() {
      return new StringBitCount();
    }
  };

  /** Thread local updates String Getbit */
  private static ThreadLocal<StringGetBit> stringGetbit = new ThreadLocal<StringGetBit>() {
    @Override
    protected StringGetBit initialValue() {
      return new StringGetBit();
    }
  };

  /** Thread local updates String SetBit */
  private static ThreadLocal<StringSetBit> stringSetbit = new ThreadLocal<StringSetBit>() {
    @Override
    protected StringSetBit initialValue() {
      return new StringSetBit();
    }
  };

  /** Thread local updates String Length */
  private static ThreadLocal<StringLength> stringLength = new ThreadLocal<StringLength>() {
    @Override
    protected StringLength initialValue() {
      return new StringLength();
    }
  };

  /** Thread local updates String GetRange */
  private static ThreadLocal<StringGetRange> stringGetrange = new ThreadLocal<StringGetRange>() {
    @Override
    protected StringGetRange initialValue() {
      return new StringGetRange();
    }
  };

  /** Thread local updates String GetSet */
  private static ThreadLocal<StringGetSet> stringGetset = new ThreadLocal<StringGetSet>() {
    @Override
    protected StringGetSet initialValue() {
      return new StringGetSet();
    }
  };

  /** Thread local updates String SetGet */
  private static ThreadLocal<StringSetGet> stringSetget = new ThreadLocal<StringSetGet>() {
    @Override
    protected StringSetGet initialValue() {
      return new StringSetGet();
    }
  };

  /** Thread local updates String GetSet */
  private static ThreadLocal<StringGetDelete> stringGetdel = new ThreadLocal<StringGetDelete>() {
    @Override
    protected StringGetDelete initialValue() {
      return new StringGetDelete();
    }
  };

  /** Thread local updates String Set */
  private static ThreadLocal<StringSet> stringSet = new ThreadLocal<StringSet>() {
    @Override
    protected StringSet initialValue() {
      return new StringSet();
    }
  };

  /** Thread local updates String SetRange */
  private static ThreadLocal<StringSetRange> stringSetrange = new ThreadLocal<StringSetRange>() {
    @Override
    protected StringSetRange initialValue() {
      return new StringSetRange();
    }
  };

  /** Thread local updates String BitPos */
  private static ThreadLocal<StringBitPos> stringBitpos = new ThreadLocal<StringBitPos>() {
    @Override
    protected StringBitPos initialValue() {
      return new StringBitPos();
    }
  };

  /** Thread local updates String GETEX */
  private static ThreadLocal<StringGetEx> stringGetex = new ThreadLocal<StringGetEx>() {
    @Override
    protected StringGetEx initialValue() {
      return new StringGetEx();
    }
  };

  /** Thread local updates String GETEXPIRE */
  private static ThreadLocal<StringGetExpire> stringGetexpire = new ThreadLocal<StringGetExpire>() {
    @Override
    protected StringGetExpire initialValue() {
      return new StringGetExpire();
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
   * Checks value arena size
   * @param required size
   */
  static void checkValueArena(int required) {
    int size = valueArenaSize.get();
    if (size >= required) {
      return;
    }
    long ptr = UnsafeAccess.realloc(valueArena.get(), required);
    valueArena.set(ptr);
    valueArenaSize.set(required);
  }

  /**
   * Build key for String. It uses thread local key arena
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size
   */
  private static int buildKey(long keyPtr, int keySize) {
    checkKeyArena(keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte) DataType.STRING.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    return kSize;
  }

  /**
   * Gets and initializes Key
   * @param ptr key address
   * @param size key size
   * @return key instance
   */
  private static Key getKey(long ptr, int size) {
    Key k = key.get();
    k.address = ptr;
    k.length = size;
    return k;
  }

  /**
   * Checks if key exists
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @return true if - yes, false - otherwise
   */
  public static boolean keyExists(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize);
    return map.exists(keyArena.get(), kSize);
  }

  /**
   * Gets key's expiration time
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @return expiration time (-1 - not found, 0 - no expire)
   */
  public static long GETEXPIRE(BigSortedMap map, long keyPtr, int keySize) {
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringGetExpire expire = stringGetexpire.get();
      expire.reset();
      expire.setKeyAddress(keyArena.get());
      expire.setKeySize(kSize);
      if (map.execute(expire)) {
        return expire.getExpire();
      } else {
        return -1;
      }
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * If key already exists and is a string, this command appends the value at the end of the string.
   * If key does not exist it is created and set as an empty string, so APPEND will be similar to
   * SET in this special case.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return the length of the string after the append operation (Integer).
   */
  public static int APPEND(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize) {

    Key kk = getKey(keyPtr, keySize);

    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringAppend append = stringAppend.get();
      append.reset();
      append.setKeyAddress(keyArena.get());
      append.setKeySize(kSize);
      append.setAppendValue(valuePtr, valueSize);
      boolean result = map.execute(append);
      if (result) {
        return append.getSizeAfterAppend();
      } else {
        // should not be here
        return -1;
      }
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Count the number of set bits (population counting) in a string. By default all the bytes
   * contained in the string are examined. It is possible to specify the counting operation only in
   * an interval passing the additional arguments start and end. Like for the GETRANGE command start
   * and end can contain negative values in order to index bytes starting from the end of the
   * string, where -1 is the last byte, -2 is the penultimate, and so forth. Non-existent keys are
   * treated as empty strings, so the command will return zero.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @param start start offset(inclusive)
   * @param end end offset (inclusive), if Common.NULL_LONG - unspecified
   * @return number of bits set or 0, if key does not exists
   */
  public static long BITCOUNT(BigSortedMap map, long keyPtr, int keySize, long start, long end) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringBitCount bitcount = stringBitcount.get();
      bitcount.reset();
      bitcount.setKeyAddress(keyArena.get());
      bitcount.setKeySize(kSize);
      bitcount.setStartEnd(start, end);
      map.execute(bitcount);
      return bitcount.getBitCount();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * TODO: this code can be optimized by avoiding unnecessary GET call Increments the number stored
   * at key by increment. If the key does not exist, it is set to 0 before performing the operation.
   * An error is returned if the key contains a value of the wrong type or contains a string that
   * can not be represented as integer. This operation is limited to 64 bit signed integers.
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param value increment value
   * @return value after increment
   * @throws OperationFailedException
   */
  public static long INCRBY(BigSortedMap map, long keyPtr, int keySize, long incr)
      throws OperationFailedException {
    Key k = getKey(keyPtr, keySize);
    try {
      writeLock(k);
      long value = 0;
      long size = GET(map, keyPtr, keySize, incrArena.get(), INCR_ARENA_SIZE);
      if (size > INCR_ARENA_SIZE) {
        throw new OperationFailedException();
      } else if (size > 0) {
        try {
          value = Utils.strToLong(incrArena.get(), (int) size);
        } catch (NumberFormatException e) {
          throw new OperationFailedException("Value at key is not a number");
        }
      }
      value += incr;
      size = Utils.longToStr(value, incrArena.get(), INCR_ARENA_SIZE);

      // Execute single SET
      int kSize = buildKey(keyPtr, keySize);
      map.put(keyArena.get(), kSize, incrArena.get(), (int) size, 0);
      return value;

    } finally {
      writeUnlock(k);
    }
  }

  /**
   * Increments the number stored at key by one. If the key does not exist, it is set to 0 before
   * performing the operation. An error is returned if the key contains a value of the wrong type or
   * contains a string that can not be represented as integer. This operation is limited to 64 bit
   * signed integers. Note: this is a string operation because Redis does not have a dedicated
   * integer type. The string stored at the key is interpreted as a base-10 64 bit signed integer to
   * execute the operation. Redis stores integers in their integer representation, so for string
   * values that actually hold an integer, there is no overhead for storing the string
   * representation of the integer.
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @return value of the key after increment
   * @throws OperationFailedException
   */
  public static long INCR(BigSortedMap map, long keyPtr, int keySize)
      throws OperationFailedException {
    return INCRBY(map, keyPtr, keySize, 1);
  }

  /**
   * Decrements the number stored at key by one. If the key does not exist, it is set to 0 before
   * performing the operation. An error is returned if the key contains a value of the wrong type or
   * contains a string that can not be represented as integer. This operation is limited to 64 bit
   * signed integers.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @return key value after decrement
   * @throws OperationFailedException
   */
  public static long DECR(BigSortedMap map, long keyPtr, int keySize)
      throws OperationFailedException {
    return INCRBY(map, keyPtr, keySize, -1);
  }

  /**
   * Decrements the number stored at key by a given value. If the key does not exist, it is set to 0
   * before performing the operation. An error is returned if the key contains a value of the wrong
   * type or contains a string that can not be represented as integer. This operation is limited to
   * 64 bit signed integers.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @param value value to decrement by
   * @return key value after decrement
   * @throws OperationFailedException
   */
  public static long DECRBY(BigSortedMap map, long keyPtr, int keySize, long value)
      throws OperationFailedException {
    return INCRBY(map, keyPtr, keySize, -value);
  }

  /**
   * TODO: optimize into single read-modify-write atomic call
   * <p>
   * Increment the string representing a floating point number stored at key by the specified
   * increment. By using a negative increment value, the result is that the value stored at the key
   * is decremented (by the obvious properties of addition). If the key does not exist, it is set to
   * 0 before performing the operation. An error is returned if one of the following conditions
   * occur: 1. The key contains a value of the wrong type (not a string). 2. The current key content
   * or the specified increment are not parsable as a double precision floating point number. If the
   * command is successful the new incremented value is stored as the new value of the key
   * (replacing the old one), and returned to the caller as a string. Both the value already
   * contained in the string key and the increment argument can be optionally provided in
   * exponential notation, however the value computed after the increment is stored consistently in
   * the same format, that is, an integer number followed (if needed) by a dot, and a variable
   * number of digits representing the decimal part of the number. Trailing zeroes are always
   * removed. The precision of the output is fixed at 17 digits after the decimal point regardless
   * of the actual internal precision of the computation.
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param incr value to increment by
   * @return key value after increment
   * @throws OperationFailedException
   */
  public static double INCRBYFLOAT(BigSortedMap map, long keyPtr, int keySize, double incr)
      throws OperationFailedException {
    Key k = getKey(keyPtr, keySize);
    try {
      writeLock(k);
      double value = 0;
      int size = (int) GET(map, keyPtr, keySize, incrArena.get(), INCR_ARENA_SIZE);
      if (size > INCR_ARENA_SIZE) {
        throw new OperationFailedException();
      } else if (size > 0) {
        try {
          value = Utils.strToDouble(incrArena.get(), size);
        } catch (NumberFormatException e) {
          throw new OperationFailedException("Value at key is not a number");
        }
      }
      value += incr;
      size = Utils.doubleToStr(value, incrArena.get(), INCR_ARENA_SIZE);
      // Execute single PUT
      int kSize = buildKey(keyPtr, keySize);

      map.put(keyArena.get(), kSize, incrArena.get(), size, 0);

      return value;
      // StringSet set = stringSet.get();
      // set.reset();
      // set.setKeyAddress(keyArena.get());
      // set.setKeySize(kSize);
      // set.setValue(incrArena.get(), size);
      // set.setMutationOptions(MutationOptions.NONE);
      // // version?
      // if(map.execute(set)) {
      // return value;
      // } else {
      // throw new OperationFailedException();
      // }
    } finally {
      writeUnlock(k);
    }
  }

  /**
   * Get the value of key. If the key does not exist the special value nil is returned. An error is
   * returned if the value stored at key is not a string, because GET only handles string values.
   * @param map sorted map
   * @param keyPtr key address
   * @param keyLength key length
   * @param valueBuf value buffer
   * @param valueBufLength value buffer size
   * @return size of a value, or -1 if not found. if size is greater than valueBufLength, the call
   *         must be repeated with appropriately sized value buffer
   */
  public static long GET(BigSortedMap map, long keyPtr, int keyLength, long valueBuf,
      int valueBufLength) {

    Key kk = getKey(keyPtr, keyLength);
    try {
      KeysLocker.readLock(kk);
      int kLength = buildKey(keyPtr, keyLength);
      long kPtr = keyArena.get();
      return map.get(kPtr, kLength, valueBuf, valueBufLength, Long.MAX_VALUE);
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * Available since 1.0.0. Time complexity: O(N) where N is the number of keys to retrieve. Returns
   * the values of all specified keys. For every key that does not hold a string value or does not
   * exist, the special value nil is returned. Because of this, the operation never fails.
   * <p>
   * Return value Array reply: list of values at the specified keys.
   * @param map sorted map
   * @param keyPtrs array of key pointers
   * @param keySizes array of key sizes
   * @param valueBuf value buffer
   * @param valueBufLength value buffer size
   * @return total serialized size of the response. If size is greater than bufferSize, the call
   *         must be repeated with appropriately sized value buffer buffer output format: INT -
   *         total entries ENTRY+
   *         <p>
   *         ENTRY: INT - size (NULL == -1) BYTES
   */
  public static long MGET(BigSortedMap map, long[] keyPtrs, int[] keySizes, long buffer,
      int bufferSize) {

    long ptr = buffer + Utils.SIZEOF_INT;

    int count = 1;
    UnsafeAccess.putInt(buffer, 0);

    for (int i = 0; i < keyPtrs.length; i++, count++) {
      int available = (int) (bufferSize - (ptr - buffer) - Utils.SIZEOF_INT);
      long size = GET(map, keyPtrs[i], keySizes[i], ptr + Utils.SIZEOF_INT, available);
      if (size <= available) {
        UnsafeAccess.putInt(buffer, count);
        UnsafeAccess.putInt(ptr, (int) size);
        // size == -1 means NULL
      }
      if (size < 0) size = 0;
      ptr += size + Utils.SIZEOF_INT;
    }
    return ptr - buffer;
  }

  /**
   * BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW
   * WRAP|SAT|FAIL]
   * <p>
   * Available since 3.2.0. Time complexity: O(1) for each subcommand specified The command treats a
   * Redis string as a array of bits, and is capable of addressing specific integer fields of
   * varying bit widths and arbitrary non (necessary) aligned offset. In practical terms using this
   * command you can set, for example, a signed 5 bits integer at bit offset 1234 to a specific
   * value, retrieve a 31 bit unsigned integer from offset 4567. Similarly the command handles
   * increments and decrements of the specified integers, providing guaranteed and well specified
   * overflow and underflow behavior that the user can configure. BITFIELD is able to operate with
   * multiple bit fields in the same command call. It takes a list of operations to perform, and
   * returns an array of replies, where each array matches the corresponding operation in the list
   * of arguments. For example the following command increments an 5 bit signed integer at bit
   * offset 100, and gets the value of the 4 bit unsigned integer at bit offset 0: > BITFIELD mykey
   * INCRBY i5 100 1 GET u4 0 1) (integer) 1 2) (integer) 0
   * <p>
   * Note that:
   * <p>
   * Addressing with GET bits outside the current string length (including the case the key does not
   * exist at all), results in the operation to be performed like the missing part all consists of
   * bits set to 0. Addressing with SET or INCRBY bits outside the current string length will
   * enlarge the string, zero-padding it, as needed, for the minimal length needed, according to the
   * most far bit touched.
   * <p>
   * Supported subcommands and integer types
   * <p>
   * The following is the list of supported commands. GET <type> <offset> -- Returns the specified
   * bit field. SET <type> <offset> <value> -- Set the specified bit field and returns its old
   * value. INCRBY <type> <offset> <increment> -- Increments or decrements (if a negative increment
   * is given) the specified bit field and returns the new value. There is another subcommand that
   * only changes the behavior of successive INCRBY subcommand calls by setting the overflow
   * behavior:
   * <p>
   * OVERFLOW [WRAP|SAT|FAIL]
   * <p>
   * Where an integer type is expected, it can be composed by prefixing with i for signed integers
   * and u for unsigned integers with the number of bits of our integer type. So for example u8 is
   * an unsigned integer of 8 bits and i16 is a signed integer of 16 bits. The supported types are
   * up to 64 bits for signed integers, and up to 63 bits for unsigned integers. This limitation
   * with unsigned integers is due to the fact that currently the Redis protocol is unable to return
   * 64 bit unsigned integers as replies.
   * <p>
   * Bits and positional offsets
   * <p>
   * There are two ways in order to specify offsets in the bitfield command. If a number without any
   * prefix is specified, it is used just as a zero based bit offset inside the string. However if
   * the offset is prefixed with a # character, the specified offset is multiplied by the integer
   * type width, so for example:
   * <p>
   * BITFIELD mystring SET i8 #0 100 SET i8 #1 200
   * <p>
   * Will set the first i8 integer at offset 0 and the second at offset 8. This way you don't have
   * to do the math yourself inside your client if what you want is a plain array of integers of a
   * given size.
   * <p>
   * Overflow control
   * <p>
   * Using the OVERFLOW command the user is able to fine-tune the behavior of the increment or
   * decrement overflow (or underflow) by specifying one of the following behaviors:
   * <p>
   * WRAP: wrap around, both with signed and unsigned integers. In the case of unsigned integers,
   * wrapping is like performing the operation modulo the maximum value the integer can contain (the
   * C standard behavior). With signed integers instead wrapping means that overflows restart
   * towards the most negative value and underflows towards the most positive ones, so for example
   * if an i8 integer is set to the value 127, incrementing it by 1 will yield -128. SAT: uses
   * saturation arithmetic, that is, on underflows the value is set to the minimum integer value,
   * and on overflows to the maximum integer value. For example incrementing an i8 integer starting
   * from value 120 with an increment of 10, will result into the value 127, and further increments
   * will always keep the value at 127. The same happens on underflows, but towards the value is
   * blocked at the most negative value. FAIL: in this mode no operation is performed on overflows
   * or underflows detected. The corresponding return value is set to NULL to signal the condition
   * to the caller.
   * <p>
   * Note that each OVERFLOW statement only affects the INCRBY commands that follow it in the list
   * of subcommands, up to the next OVERFLOW statement.
   * <p>
   * By default, WRAP is used if not otherwise specified. > BITFIELD mykey incrby u2 100 1 OVERFLOW
   * SAT incrby u2 102 1 1) (integer) 1 2) (integer) 1 > BITFIELD mykey incrby u2 100 1 OVERFLOW SAT
   * incrby u2 102 1 1) (integer) 2 2) (integer) 2 > BITFIELD mykey incrby u2 100 1 OVERFLOW SAT
   * incrby u2 102 1 1) (integer) 3 2) (integer) 3 > BITFIELD mykey incrby u2 100 1 OVERFLOW SAT
   * incrby u2 102 1 1) (integer) 0 2) (integer) 3
   * <p>
   * Return value The command returns an array with each entry being the corresponding result of the
   * sub command given at the same position. OVERFLOW subcommands don't count as generating a reply.
   * The following is an example of OVERFLOW FAIL returning NULL. > BITFIELD mykey OVERFLOW FAIL
   * incrby u2 102 1 1) (nil)
   * <p>
   * Motivations
   * <p>
   * The motivation for this command is that the ability to store many small integers as a single
   * large bitmap (or segmented over a few keys to avoid having huge keys) is extremely memory
   * efficient, and opens new use cases for Redis to be applied, especially in the field of real
   * time analytics. This use cases are supported by the ability to specify the overflow in a
   * controlled way. Fun fact: Reddit's 2017 April fools' project r/place was built using the Redis
   * BITFIELD command in order to take an in-memory representation of the collaborative canvas.
   * <p>
   * Performance considerations Usually BITFIELD is a fast command, however note that addressing far
   * bits of currently short strings will trigger an allocation that may be more costly than
   * executing the command on bits already existing. Orders of bits The representation used by
   * BITFIELD considers the bitmap as having the bit number 0 to be the most s ignificant bit of the
   * first byte, and so forth, so for example setting a 5 bits unsigned integer to value 23 at
   * offset 7 into a bitmap previously set to all zeroes, will produce the following representation:
   * +--------+--------+ |00000001|01110000| +--------+--------+ When offsets and integer sizes are
   * aligned to bytes boundaries, this is the same as big endian, however when such alignment does
   * not exist, its important to also understand how the bits inside a byte are ordered.
   * @param map
   * @param keyPtr
   * @param keySize
   * @return
   */
  public static long BITFIELD(BigSortedMap map, long keyPtr, int keySize) {
    return 0;
  }

  /**
   * Returns the bit value at offset in the string value stored at key. When offset is beyond the
   * string length, the string is assumed to be a contiguous space with 0 bits. When key does not
   * exist it is assumed to be an empty string, so offset is always out of range and the value is
   * also assumed to be a contiguous space with 0 bits.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key length
   * @param offset offset to lookup bit
   * @return 1 or 0
   */
  public static int GETBIT(BigSortedMap map, long keyPtr, int keySize, long offset) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringGetBit getbit = stringGetbit.get();
      getbit.reset();
      getbit.setKeyAddress(keyArena.get());
      getbit.setKeySize(kSize);
      getbit.setOffset(offset);
      map.execute(getbit);
      return getbit.getBit();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * Sets or clears the bit at offset in the string value stored at key. The bit is either set or
   * cleared depending on value, which can be either 0 or 1. When key does not exist, a new string
   * value is created. The string is grown to make sure it can hold a bit at offset. The offset
   * argument is required to be greater than or equal to 0, and smaller than 232 (this limits
   * bitmaps to 512MB). When the string at key is grown, added bits are set to 0. Actually we have
   * higher limit - 2GB per value
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param offset offset to set bit at
   * @param bit bit value (0 or 1)
   * @return old bit value (0 if did not exists)
   */
  public static int SETBIT(BigSortedMap map, long keyPtr, int keySize, long offset, int bit) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringSetBit setbit = stringSetbit.get();
      setbit.reset();
      setbit.setKeyAddress(keyArena.get());
      setbit.setKeySize(kSize);
      setbit.setOffset(offset);
      setbit.setBit(bit);
      map.execute(setbit);
      return setbit.getOldBit();
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Returns the length of the string value stored at key. An error is returned when key holds a
   * non-string value.
   * @param map sorted map
   * @param keyPtr key
   * @param keySize
   * @return size of a value or -1 if does not exists
   */
  public static int STRLEN(BigSortedMap map, long keyPtr, int keySize) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringLength strlen = stringLength.get();
      strlen.reset();
      strlen.setKeyAddress(keyArena.get());
      strlen.setKeySize(kSize);
      map.execute(strlen);
      return strlen.getLength();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * Returns the substring of the string value stored at key, determined by the offsets start and
   * end (both are inclusive). Negative offsets can be used in order to provide an offset starting
   * from the end of the string. So -1 means the last character, -2 the penultimate and so forth.
   * The function handles out of range requests by limiting the resulting range to the actual length
   * of the string.
   * @param map
   * @param keyPtr
   * @param keySize
   * @param start
   * @param end
   * @return size of a range, or -1, if key does not exists or limits out of a value range if size >
   *         buferSize, the call must be repeated with appropriately sized buffer
   */
  public static int GETRANGE(BigSortedMap map, long keyPtr, int keySize, long start, long end,
      long bufferPtr, int bufferSize) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringGetRange getrange = stringGetrange.get();
      getrange.reset();
      getrange.setKeyAddress(keyArena.get());
      getrange.setKeySize(kSize);
      getrange.setFromTo(start, end);
      getrange.setBuffer(bufferPtr, bufferSize);
      map.execute(getrange);
      return getrange.getRangeLength();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * Atomically sets key to value and returns the old value stored at key. Returns an error when key
   * exists but does not hold a string value.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param bufferPtr buffer address to copy old version to
   * @param bufferSize buffer size
   * @return size of an old value, -1 if did not existed. If size > bufferSize, the call must be
   *         repeated with appropriately sized buffer
   */
  public static int GETSET(BigSortedMap map, long keyPtr, int keySize, long valuePtr, int valueSize,
      long bufferPtr, int bufferSize) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringGetSet getset = stringGetset.get();
      getset.reset();
      getset.setKeyAddress(keyArena.get());
      getset.setKeySize(kSize);
      getset.setBuffer(bufferPtr, bufferSize);
      getset.setValue(valuePtr, valueSize);
      map.execute(getset);
      return getset.getPreviousVersionLength();
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Available since 6.2.0. Time complexity: O(1) Get the value of key and optionally set its
   * expiration. GETEX is similar to GET, but is a write command with additional options.
   * <p>
   * Options:
   * <p>
   * The GETEX command supports a set of options that modify its behavior: EX seconds -- Set the
   * specified expire time, in seconds. PX milliseconds -- Set the specified expire time, in
   * milliseconds. EXAT timestamp-seconds -- Set the specified Unix time at which the key will
   * expire, in seconds. PXAT timestamp-milliseconds -- Set the specified Unix time at which the key
   * will expire, in milliseconds. PERSIST -- Remove the time to live associated with the key.
   * <p>
   * Return value: Bulk string reply: the value of key, or nil when key does not exist.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size in bytes
   * @param expireAt set expiration time (if == 0, then unexpire)
   * @param bufferPtr buffer address
   * @param bufferSize buffer size
   * @return size of a value or -1
   */
  public static int GETEX(BigSortedMap map, long keyPtr, int keySize, long expireAt, long bufferPtr,
      int bufferSize) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringGetEx getex = stringGetex.get();
      getex.reset();
      getex.setKeyAddress(keyArena.get());
      getex.setKeySize(kSize);
      getex.setBuffer(bufferPtr, bufferSize);
      getex.setExpire(expireAt);
      if (map.execute(getex)) {
        return getex.getValueLength();
      } else {
        return -1;
      }
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Available since 6.2.0. Time complexity: O(1) Get the value of key and delete the key. This
   * command is similar to GET, except for the fact that it also deletes the key on success (if and
   * only if the key's value type is a string). Return value: Bulk string reply: the value of key,
   * nil when key does not exist, or an error if the key's value type isn't a string.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param bufferPtr buffer address (for return value)
   * @param bufferSize buffer size
   * @return size of a value or -1 if does not exists
   */
  public static int GETDEL(BigSortedMap map, long keyPtr, int keySize, long bufferPtr,
      int bufferSize) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringGetDelete getdel = stringGetdel.get();
      getdel.reset();
      getdel.setKeyAddress(keyArena.get());
      getdel.setKeySize(kSize);
      getdel.setBuffer(bufferPtr, bufferSize);
      map.execute(getdel);
      return getdel.getValueLength();
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Set key to hold the string value and returns previous value. If key already holds a value, it
   * is overwritten, regardless of its type. Any previous time to live associated with the key is
   * discarded on successful SET operation (if keepTTL == false). This call covers the following
   * commands: SET, SETNX, SETEX, PSETEX with GET option
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time (0 - does not expire)
   * @param opts mutation options (NONE, NX, XX)
   * @param keepTTL keep current TTL
   * @return length of an old value
   */
  public static long SETGET(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize, long expire, MutationOptions opts, boolean keepTTL, long bufPtr, int bufSize) {

    Key kk = getKey(keyPtr, keySize);

    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringSetGet set = stringSetget.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setValue(valuePtr, valueSize);
      set.setKeepTTL(keepTTL);
      set.setMutationOptions(opts);
      set.setExpire(expire);
      set.setBuffer(bufPtr, bufSize);
      boolean result = map.execute(set);
      return set.getOldValueSize();
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Set key to hold the string value. If key already holds a value, it is overwritten, regardless
   * of its type. Any previous time to live associated with the key is discarded on successful SET
   * operation (if keepTTL == false). This call covers the following commands: SET, SETNX, SETEX,
   * PSETEX
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time (0 - does not expire)
   * @param opts mutation options (NONE, NX, XX)
   * @param keepTTL keep current TTL
   * @return true on success, false - otherwise
   */
  public static boolean SET(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize, long expire, MutationOptions opts, boolean keepTTL) {

    if (expire == 0 && opts == MutationOptions.NONE && keepTTL == false) {
      return SET_DIRECT(map, keyPtr, keySize, valuePtr, valueSize);
    }
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringSet set = stringSet.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setValue(valuePtr, valueSize);
      set.setKeepTTL(keepTTL);
      set.setMutationOptions(opts);
      set.setExpire(expire);
      boolean result = map.execute(set);
      return result;
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * This optimized version is used for MSET (no expire, no mutation options)
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true - success, false - OOM
   */
  public static boolean SET_DIRECT(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      return map.put(kPtr, kSize, valuePtr, valueSize, 0);
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * DELETE key
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @return true- success, false - otherwise
   */
  public static boolean DELETE(BigSortedMap map, long keyPtr, int keySize) {
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      return map.delete(keyArena.get(), kSize);
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Set key to hold string value if key does not exist. In that case, it is equal to SET. When key
   * already holds a value, no operation is performed. SETNX is short for "SET if Not eXists".
   * Return value Integer reply, specifically: 1 if the key was set 0 if the key was not set
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time (0 - no expire)
   * @return true on success, false - otherwise
   */
  public static boolean SETNX(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize, long expire) {
    return SET(map, keyPtr, keySize, valuePtr, valueSize, expire, MutationOptions.NX, false);
  }

  /**
   * Set key to hold string value if key exist only. In that case, it is equal to SET. When key does
   * not exist, no operation is performed. SETXX is short for "SET if eXXists". Return value Integer
   * reply, specifically: 1 if the key was set 0 if the key was not set
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time (0 - no expire)
   * @return true on success, false - otherwise
   */
  public static boolean SETXX(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize, long expire) {
    return SET(map, keyPtr, keySize, valuePtr, valueSize, expire, MutationOptions.XX, false);
  }

  /**
   * Set key to hold the string value and set key to timeout after a given number of seconds. This
   * command is equivalent to executing the following commands: SET mykey value EXPIRE mykey seconds
   * SETEX is atomic, and can be reproduced by using the previous two commands inside an MULTI /
   * EXEC block. It is provided as a faster alternative to the given sequence of operations, because
   * this operation is very common when Redis is used as a cache. An error is returned when seconds
   * is invalid.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time
   * @return true, false
   */
  public static boolean SETEX(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize, long expire) {
    return SET(map, keyPtr, keySize, valuePtr, valueSize, expire, MutationOptions.NONE, false);
  }

  /**
   * Set key to hold the string value and set key to timeout after a given number of milliseconds.
   * This command is equivalent to executing the following commands: SET mykey value EXPIRE mykey
   * seconds SETEX is atomic, and can be reproduced by using the previous two commands inside an
   * MULTI / EXEC block. It is provided as a faster alternative to the given sequence of operations,
   * because this operation is very common when Redis is used as a cache. An error is returned when
   * seconds is invalid.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time
   * @return true, false
   */
  public static boolean PSETEX(BigSortedMap map, long keyPtr, int keySize, long valuePtr,
      int valueSize, long expire) {
    return SET(map, keyPtr, keySize, valuePtr, valueSize, expire, MutationOptions.NONE, false);
  }

  /**
   * Overwrites part of the string stored at key, starting at the specified offset, for the entire
   * length of value. If the offset is larger than the current length of the string at key, the
   * string is padded with zero-bytes to make offset fit. Non-existing keys are considered as empty
   * strings, so this command will make sure it holds a string large enough to be able to set value
   * at offset. Note that the maximum offset that you can set is 229 -1 (536870911), as Redis
   * Strings are limited to 512 megabytes. If you need to grow beyond this size, you can use
   * multiple keys.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param offset offset to set value
   * @param valuePtr value address
   * @param valueSize value size
   * @return new size of key's value (-1 if operation was not performed)
   */
  public static long SETRANGE(BigSortedMap map, long keyPtr, int keySize, long offset,
      long valuePtr, int valueSize) {
    // Sanity check
    if (offset < 0) return -1;
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringSetRange setrange = stringSetrange.get();
      setrange.reset();
      setrange.setKeyAddress(keyArena.get());
      setrange.setKeySize(kSize);
      setrange.setValue(valuePtr, valueSize);
      setrange.setOffset(offset);
      map.execute(setrange);
      return setrange.getValueLength();
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }

  /**
   * Return the position of the first bit set to 1 or 0 in a string. The position is returned,
   * thinking of the string as an array of bits from left to right, where the first byte's most
   * significant bit is at position 0, the second byte's most significant bit is at position 8, and
   * so forth. The same bit position convention is followed by GETBIT and SETBIT. By default, all
   * the bytes contained in the string are examined. It is possible to look for bits only in a
   * specified interval passing the additional arguments start and end (it is possible to just pass
   * start, the operation will assume that the end is the last byte of the string. However there are
   * semantic differences as explained later). The range is interpreted as a range of bytes and not
   * a range of bits, so start=0 and end=2 means to look at the first three bytes. Note that bit
   * positions are returned always as absolute values starting from bit zero even when start and end
   * are used to specify a range. Like for the GETRANGE command start and end can contain negative
   * values in order to index bytes starting from the end of the string, where -1 is the last byte,
   * -2 is the penultimate, and so forth. Non-existent keys are treated as empty strings.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param bit bit value to look for
   * @param start start offset (in bytes) inclusive
   * @param end end position (in bytes) inclusive, if Commons.NULL_LONG - means unspecified
   * @return The command returns the position of the first bit set to 1 or 0 according to the
   *         request. If we look for set bits (the bit argument is 1) and the string is empty or
   *         composed of just zero bytes, -1 is returned. If we look for clear bits (the bit
   *         argument is 0) and the string only contains bit set to 1, the function returns the
   *         first bit not part of the string on the right. So if the string is three bytes set to
   *         the value 0xff the command BITPOS key 0 will return 24, since up to bit 23 all the bits
   *         are 1. Basically, the function considers the right of the string as padded with zeros
   *         if you look for clear bits and specify no range or the start argument only. However,
   *         this behavior changes if you are looking for clear bits and specify a range with both
   *         start and end. If no clear bit is found in the specified range, the function returns -1
   *         as the user specified a clear range and there are no 0 bits in that range.
   */
  public static long BITPOS(BigSortedMap map, long keyPtr, int keySize, int bit, long start,
      long end) {

    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize);
      StringBitPos bitcount = stringBitpos.get();
      bitcount.reset();
      bitcount.setKeyAddress(keyArena.get());
      bitcount.setKeySize(kSize);
      bitcount.setStartEnd(start, end);
      bitcount.setBit(bit);
      map.execute(bitcount);
      return bitcount.getPosition();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }

  /**
   * TODO: this is not atomic unfortunately, b/c scan SCAN operation can get access to K-Vs during
   * this operation We would say - its partially atomic
   * <p>
   * Sets the given keys to their respective values. MSET replaces existing values with new values,
   * just as regular SET. See MSETNX if you don't want to overwrite existing values. MSET is atomic,
   * so all given keys are set at once. It is not possible for clients to see that some of the keys
   * were updated while others are unchanged.
   * @param map sorted map storage
   * @param kvs list of key-values to set
   */
  public static boolean MSET(BigSortedMap map, List<KeyValue> kvs) {

    try {
      KeysLocker.writeLockAllKeyValues(kvs);
      for (int i = 0; i < kvs.size(); i++) {
        KeyValue kv = kvs.get(i);
        boolean result = SET_DIRECT(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
        if (!result) {
          // Out of memory possible
          return false;
        }
      }
      return true;
    } finally {
      KeysLocker.writeUnlockAllKeyValues(kvs);
    }
  }

  /**
   * TODO: the same partial atomicity as above Sets the given keys to their respective values.
   * MSETNX will not perform any operation at all even if just a single key already exists. Because
   * of this semantic MSETNX can be used in order to set different keys representing different
   * fields of an unique logic object in a way that ensures that either all the fields or none at
   * all are set. MSETNX is atomic, so all given keys are set at once. It is not possible for
   * clients to see that some of the keys were updated while others are unchanged. Return value
   * Integer reply, specifically: 1 if the all the keys were set. 0 if no key was set (at least one
   * key already existed).
   * @param map sorted map storage
   * @param kvs list of key-values to set
   * @return true on success, false - otherwise
   */
  public static boolean MSETNX(BigSortedMap map, List<KeyValue> kvs) {

    try {
      KeysLocker.writeLockAllKeyValues(kvs);
      for (int i = 0; i < kvs.size(); i++) {
        KeyValue kv = kvs.get(i);
        if (keyExists(map, kv.keyPtr, kv.keySize)) {
          return false;
        }
      }
      for (int i = 0; i < kvs.size(); i++) {
        KeyValue kv = kvs.get(i);
        SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, false);
      }
    } finally {
      KeysLocker.writeUnlockAllKeyValues(kvs);
    }
    return true;
  }

  /**
   * Available since 2.6.0. Time complexity: O(N) Perform a bitwise operation between multiple keys
   * (containing string values) and store the result in the destination key. The BITOP command
   * supports four bitwise operations: AND, OR, XOR and NOT, thus the valid forms to call the
   * command are:
   * <p>
   * BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN BITOP OR destkey srckey1 srckey2 srckey3
   * ... srckeyN BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN BITOP NOT destkey srckey
   * <p>
   * As you can see NOT is special as it only takes an input key, because it performs inversion of
   * bits so it only makes sense as an unary operator. The result of the operation is always stored
   * at destkey.
   * <p>
   * Handling of strings with different lengths
   * <p>
   * When an operation is performed between strings having different lengths, all the strings
   * shorter than the longest string in the set are treated as if they were zero-padded up to the
   * length of the longest string. The same holds true for non-existent keys, that are considered as
   * a stream of zero bytes up to the length of the longest string.
   * <p>
   * Return value Integer reply The size of the string stored in the destination key, that is equal
   * to the size of the longest input string.
   * <p>
   * Examples:
   * <p>
   * redis> SET key1 "foobar" "OK" redis> SET key2 "abcdef" "OK" redis> BITOP AND dest key1 key2
   * (integer) 6 redis> GET dest "`bc`ab" redis>
   * <p>
   * Pattern: real time metrics using bitmaps
   * <p>
   * BITOP is a good complement to the pattern documented in the BITCOUNT command documentation.
   * Different bitmaps can be combined in order to obtain a target bitmap where the population
   * counting operation is performed. See the article called "Fast easy realtime metrics using Redis
   * bitmaps" for a interesting use cases.
   * <p>
   * Performance considerations BITOP is a potentially slow command as it runs in O(N) time. Care
   * should be taken when running it against long input strings. For real-time metrics and
   * statistics involving large inputs a good approach is to use a replica (with read-only option
   * disabled) where the bit-wise operations are performed to avoid blocking the master instance.
   * @param map sorted map storage
   * @param op bitwise operation (AND, XOR, OR, NOT)
   * @param keyPtrs array of key pointers (first key is the destination key)
   * @param keySizes array of key sizes
   * @return size of a destination string in bytes
   */
  public static long BITOP(BigSortedMap map, BitOp op, long[] keyPtrs, int[] keySizes) {
    // TODO
    return 0;
  }
}
