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
package com.carrotdata.redcarrot.redis.sparse;

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
public class SparseGetBit extends Operation {

  long offset;
  int bit = 0;

  public SparseGetBit() {
    setFloorKey(true);
    setReadOnly(true);
  }

  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (Utils.compareTo(
            foundKeyPtr, foundKeySize - Utils.SIZEOF_LONG, keyAddress, keySize - Utils.SIZEOF_LONG)
        != 0) {
      // sparse bitmap Key not found
      return true;
    }
    long foundChunkOffset = SparseBitmaps.getChunkOffsetFromKey(foundKeyPtr, foundKeySize);
    if (this.offset >= foundChunkOffset + SparseBitmaps.BITS_PER_CHUNK) {
      // Not found => =0
      return true;
    }
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    boolean isCompressed = SparseBitmaps.isCompressed(valuePtr);
    if (isCompressed) {
      valuePtr = SparseBitmaps.decompress(valuePtr, valueSize - SparseBitmaps.HEADER_SIZE);
    }

    this.bit = getbit(valuePtr);
    return true;
  }

  private int getbit(long valuePtr) {
    long chunkOffset = this.offset / SparseBitmaps.BITS_PER_CHUNK;
    int off = (int) (this.offset - chunkOffset * SparseBitmaps.BITS_PER_CHUNK);
    int n = (int) (off / Utils.BITS_PER_BYTE);
    int rem = (int) (off - ((long) n) * Utils.BITS_PER_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n + SparseBitmaps.HEADER_SIZE);
    return (b >>> (7 - rem)) & 1;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.offset = 0;
    setFloorKey(true);
    setReadOnly(true);
  }

  /**
   * Set offset for this operation
   *
   * @param offset offset in bits
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }
  /**
   * Returns bit value at offset
   *
   * @return bit value: 0 or 1
   */
  public int getBit() {
    return bit;
  }
}
