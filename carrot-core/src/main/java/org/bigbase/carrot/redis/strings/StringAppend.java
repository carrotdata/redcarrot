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
package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * String append mutation. If key already exists and is a string, this command appends the value at
 * the end of the string. If key does not exist it is created and set as an empty string, so APPEND
 * will be similar to SET in this special case.
 */
public class StringAppend extends Operation {

  /*
   * Append value address
   */
  long appendValuePtr;
  /*
   * Append value size
   */
  int appendValueSize;
  /*
   * Size of a value after append
   */
  int sizeAfterAppend;

  /*
   * Thread local buffer
   * Optimized syntax for java 8
   */
  static ThreadLocal<Long> buffer = ThreadLocal.withInitial(() -> UnsafeAccess.malloc(4096));

  /*
   * Thread local buffer size
   * Optimized syntax for java 8
   */
  static ThreadLocal<Integer> bufferSize = ThreadLocal.withInitial(() -> 4096);

  public StringAppend() {
    // WTF is it for?
    // setFloorKey(true);
  }

  @Override
  public void reset() {
    super.reset();
    appendValuePtr = 0;
    appendValueSize = 0;
    sizeAfterAppend = 0;
  }
  /**
   * Sets append value address and size
   *
   * @param ptr append value address
   * @param size append value size
   */
  public void setAppendValue(long ptr, int size) {
    this.appendValuePtr = ptr;
    this.appendValueSize = size;
  }

  /**
   * Gets value size after append
   *
   * @return value size after append operation
   */
  public int getSizeAfterAppend() {
    return this.sizeAfterAppend;
  }

  /**
   * Ensure enough space in a thread local buffer
   *
   * @param required required size
   */
  private void checkBuffer(int required) {
    int size = bufferSize.get();
    if (size >= required) {
      return;
    }
    UnsafeAccess.free(buffer.get());
    long ptr = UnsafeAccess.malloc(required);
    buffer.set(ptr);
    bufferSize.set(required);
  }

  @Override
  public boolean execute() {
    int vsize = appendValueSize;
    if (foundRecordAddress > 0) {
      vsize = DataBlock.valueLength(foundRecordAddress);
      checkBuffer(vsize + appendValueSize);
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      long bufferPtr = buffer.get();
      UnsafeAccess.copy(ptr, bufferPtr, vsize);
      UnsafeAccess.copy(appendValuePtr, bufferPtr + vsize, appendValueSize);
      vsize += appendValueSize;
      // set key
      keys[0] = keyAddress; // original key
      keySizes[0] = keySize;
      values[0] = bufferPtr;
      valueSizes[0] = vsize;
    } else {
      // set key
      keys[0] = keyAddress; // original key
      keySizes[0] = keySize;
      values[0] = appendValuePtr;
      valueSizes[0] = appendValueSize;
    }
    this.sizeAfterAppend = vsize;
    // Set update count to 1
    updatesCount = 1;
    return true;
  }
}
