/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.util;

import java.io.IOException;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.BigSortedMapScanner;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class Commons {
  public static final long ZERO = UnsafeAccess.malloc(1);

  static {
    UnsafeAccess.putByte(ZERO, (byte) 0);
  }

  public static final int NULL_INT = Integer.MIN_VALUE;
  public static final long NULL_LONG = Long.MIN_VALUE;
  /*
   * Number of bytes to keep sizes of element in Value object
   */
  public static final int NUM_ELEM_SIZE = Utils.SIZEOF_SHORT;
  /*
   * Key size, currently 4 bytes
   */
  public static final int KEY_SIZE = Utils.SIZEOF_INT;

  /*
   * Key prefix size (5 bytes)
   */
  public static final int KEY_PREFIX_SIZE = KEY_SIZE + Utils.SIZEOF_BYTE;

  /**
   * Length (size) of a Key
   * @param key address
   * @return length of key
   */
  public static int keySize(long keyAddress) {
    return UnsafeAccess.toInt(keyAddress + Utils.SIZEOF_BYTE);
  }

  /**
   * Length of a Key with prefix
   * @param keyAddress key address
   * @return length with prefix
   */
  public static int keySizeWithPrefix(long keyAddress) {
    return UnsafeAccess.toInt(keyAddress + Utils.SIZEOF_BYTE) + KEY_SIZE + Utils.SIZEOF_BYTE;
  }

  /**
   * Checks if a given Key is the first one in a type (set, hash, etc) First key has the following
   * format: [KEY_SIZE]Key'0' - zero at the end
   * @param ptr key address
   * @param size key size
   * @return true or false
   */
  public static boolean firstKVinType(long ptr, int size) {
    if (keySize(ptr) + KEY_SIZE + Utils.SIZEOF_BYTE + 1 != size) {
      return false;
    }
    return UnsafeAccess.toByte(ptr + size - 1) == 0;
  }

  /**
   * Gets element (field) address from mutation key
   * @param keyAddress key address
   * @return address of element
   */
  public static long elementAddressFromKey(long keyAddress) {
    // Read set key size 4 bytes
    int setKeySize = keySize(keyAddress);
    return keyAddress + KEY_SIZE + setKeySize + Utils.SIZEOF_BYTE;
  }

  /**
   * Gets element (field) size from a mutation key
   * @param keyAddress key address
   * @param keySize size of a key
   * @return size of an element
   */
  public static int elementSizeFromKey(long keyAddress, int keySize) {
    // Read set key size 4 bytes
    int setKeySize = keySize(keyAddress);
    return keySize - KEY_SIZE - setKeySize - Utils.SIZEOF_BYTE;
  }

  /**
   * Number of elements(field-value pairs) in a Value object
   * @param valuePtr address
   * @return number of elements
   */
  public static int numElementsInValue(long valuePtr) {
    return UnsafeAccess.toShort(valuePtr);
  }

  /**
   * Increase number of elements (fields-values) in a Value object
   * @param valuePtr value address
   * @param v value to increase
   * @return total new number of elements
   */
  public static int addNumElements(long valuePtr, int v) {
    int value = UnsafeAccess.toShort(valuePtr);
    // if (value + v < 0) return value;
    UnsafeAccess.putShort(valuePtr, (short) (value + v));
    return value + v;
  }

  /**
   * Set number of elements (field-values) in a Value object
   * @param valuePtr value address
   * @param v value to set to
   * @return v new number of elements
   */
  public static int setNumElements(long valuePtr, int v) {
    UnsafeAccess.putShort(valuePtr, (short) (v));
    return v;
  }

  /**
   * This method checks if next K-V exists in the set/hash/list
   * @param ptr current key address
   * @return true if exists, false - otherwise
   */
  public static boolean nextKVisInType(BigSortedMap map, long ptr) {
    int keySize = keySize(ptr) + KEY_SIZE;
    BigSortedMapScanner scanner = map.getPrefixScanner(ptr, keySize);
    try {
      // should not be null
      return scanner.next();
    } finally {
      try {
        scanner.close();
      } catch (IOException e) {
        // swallow
      }
    }
  }

  /**
   * Checks if Value object can be split. (number of element is greater than 1)
   * @param valuePtr address
   * @return true, if - yes, false otherwise
   */
  public static boolean canSplit(long valuePtr) {
    return numElementsInValue(valuePtr) > 1;
  }

  public static boolean isFirstKey(long foundKeyAddress, int foundKeySize, int keySize) {
    /*
     * First key has the following format data type (1 byte) original key size (4 bytes) original
     * key '\0' - ZERO suffix
     */
    int firstKeySize = keySize + 2 * Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    if (foundKeySize > firstKeySize || foundKeySize < firstKeySize) {
      return false;
    }
    return UnsafeAccess.toByte(foundKeyAddress + foundKeySize - 1) == 0;
  }

  public static long countRecords(BigSortedMap map) throws IOException {
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long count = 0;
    while (scanner.hasNext()) {
      count++;
      scanner.next();
    }
    scanner.close();
    return count;
  }
}
