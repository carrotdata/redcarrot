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

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.ops.Operation;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * String GETEX operation. Atomically gets key value and updates expire. Returns an error when key
 * exists but does not hold a string value.
 */
public class StringGetEx extends Operation {

  private long bufferPtr;
  private int bufferSize;
  private int size = -1; // means did not exist

  @Override
  public boolean execute() {
    setUpdateInPlace(true);
    if (foundRecordAddress > 0) {
      int vLength = DataBlock.valueLength(foundRecordAddress);
      this.size = vLength;
      if (this.size > this.bufferSize) {
        this.updatesCount = 0;
        return false;
      }
      long vPtr = DataBlock.valueAddress(foundRecordAddress);
      UnsafeAccess.copy(vPtr, bufferPtr, vLength);
      // Update expire field
      // FIXME: does not work when compression is on, b/c we update only
      // thread-local buffer
      DataBlock.setRecordExpire(foundRecordAddress, this.expire);
    } else {
      // Does not exist
      this.updatesCount = 0;
      return false;
    }
    return true;
  }

  public void setBuffer(long ptr, int size) {
    this.bufferPtr = ptr;
    this.bufferSize = size;
  }

  /**
   * Get value previous size
   * @return size
   */
  public int getValueLength() {
    return this.size;
  }

  @Override
  public void reset() {
    super.reset();
    this.bufferPtr = 0;
    this.bufferSize = 0;
    this.size = -1; // means did not existed
  }
}
