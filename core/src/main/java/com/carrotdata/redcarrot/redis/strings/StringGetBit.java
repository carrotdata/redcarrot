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
package com.carrotdata.redcarrot.redis.strings;

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.ops.Operation;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

/**
 * Get bit by offset operation. Returns the bit value at offset in the string value stored at key.
 * When offset is beyond the string length, the string is assumed to be a contiguous space with 0
 * bits. When key does not exist it is assumed to be an empty string, so offset is always out of
 * range and the value is also assumed to be a contiguous space with 0 bits.
 */
public class StringGetBit extends Operation {

  long offset;
  int bit;

  public StringGetBit() {
    setReadOnly(true);
  }

  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }

    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    if (offset < 0 || offset > ((long) valueSize) * Utils.BITS_PER_BYTE - 1) {
      return true;
    }
    this.bit = getbit(valuePtr, valueSize);
    return true;
  }

  private int getbit(long valuePtr, int valueSize) {
    int n = (int) (this.offset / Utils.BITS_PER_BYTE);
    int rem = (int) (offset - ((long) n) * Utils.BITS_PER_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n);
    return (b >>> (7 - rem)) & 1;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.offset = 0;
    setReadOnly(true);
  }

  /**
   * Set offset for this operation
   * @param offset offset in bits
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Returns bit value at offset
   * @return bit alue: 0 or 1
   */
  public int getBit() {
    return bit;
  }
}
