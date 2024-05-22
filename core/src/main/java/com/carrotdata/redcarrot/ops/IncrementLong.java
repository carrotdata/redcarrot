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
package com.carrotdata.redcarrot.ops;

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

/** This example of specific Update - atomic counter */
public class IncrementLong extends Operation {

  static ThreadLocal<Long> buffer =
      new ThreadLocal<Long>() {
        @Override
        protected Long initialValue() {
          return UnsafeAccess.malloc(Utils.SIZEOF_LONG);
        }
      };

  long value;

  public IncrementLong() {
    setReadOnly(false);
    setUpdateInPlace(true);
  }

  /**
   * Set increment value
   *
   * @param v value
   */
  public void setIncrement(long v) {
    this.value = v;
  }

  /**
   * Get increment value
   *
   * @return
   */
  public long getIncrement() {
    return this.value;
  }
  /**
   * Get value after increment
   *
   * @return value after increment
   */
  public long getValue() {
    return this.value;
  }

  @Override
  public void reset() {
    super.reset();
    value = 0;
    setReadOnly(false);
    setUpdateInPlace(true);
  }

  @Override
  public boolean execute() {
    long v = 0;
    if (foundRecordAddress > 0) {
      int vSize = DataBlock.valueLength(foundRecordAddress);
      if (vSize != Utils.SIZEOF_LONG /*long size*/) {
        return false;
      }
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      v = UnsafeAccess.toLong(ptr);
      this.value += v;
      UnsafeAccess.putLong(ptr, value);
      this.updatesCount = 0;
      return true;
    }
    setUpdateInPlace(false);
    UnsafeAccess.putLong(buffer.get(), value);
    // set updateCounts to 1 - we insert new k-v
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = buffer.get();
    this.valueSizes[0] = Utils.SIZEOF_LONG;
    return true;
  }
}
