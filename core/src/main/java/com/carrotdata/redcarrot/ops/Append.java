/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.ops;

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.util.UnsafeAccess;

public class Append extends Operation {

  long appendValuePtr;
  int appendValueSize;

  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      long ptr = UnsafeAccess.malloc(4096);
      return ptr;
    }
  };

  static ThreadLocal<Integer> bufferSize = new ThreadLocal<Integer>() {

    @Override
    protected Integer initialValue() {
      return 4096;
    }
  };

  public Append() {
    updateInPlace = false;
    readOnly = false;
  }

  @Override
  public void reset() {
    super.reset();
    appendValuePtr = 0;
    appendValueSize = 0;
    updateInPlace = false;
    readOnly = false;
  }

  public void setAppendValue(long ptr, int size) {
    this.appendValuePtr = ptr;
    this.appendValueSize = size;
  }

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
    if (foundRecordAddress <= 0) {
      return false;
    }
    int vsize = DataBlock.valueLength(foundRecordAddress);
    checkBuffer(vsize + appendValueSize);
    long ptr = DataBlock.valueAddress(foundRecordAddress);
    long bufferPtr = buffer.get();
    UnsafeAccess.copy(ptr, bufferPtr, vsize);
    UnsafeAccess.copy(appendValuePtr, bufferPtr + vsize, appendValueSize);
    // set key
    keys[0] = keyAddress; // original key
    keySizes[0] = keySize;
    values[0] = bufferPtr;
    valueSizes[0] = vsize + appendValueSize;
    // Set update count to 1
    updatesCount = 1;
    return true;
  }
}
