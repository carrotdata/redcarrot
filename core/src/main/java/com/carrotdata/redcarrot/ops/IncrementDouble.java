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
package com.carrotdata.redcarrot.ops;

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

/** This example of specific Update - atomic counter */
public class IncrementDouble extends Operation {

  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
    }
  };

  double value;

  public IncrementDouble() {
    setReadOnly(false);
    setUpdateInPlace(true);
  }

  public void setIncrement(double v) {
    this.value = v;
  }

  public double getIncrement() {
    return this.value;
  }

  public double getValue() {
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
    double dv = 0;
    if (foundRecordAddress > 0) {
      int vSize = DataBlock.valueLength(foundRecordAddress);
      if (vSize != Utils.SIZEOF_DOUBLE /* long size */) {
        return false;
      }
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      long v = UnsafeAccess.toLong(ptr);
      dv = Double.longBitsToDouble(v);
      value += dv;
      UnsafeAccess.putLong(ptr, Double.doubleToLongBits(value));
      this.updatesCount = 0;
      return true;
    }
    setUpdateInPlace(false);
    value += dv;
    UnsafeAccess.putLong(buffer.get(), Double.doubleToLongBits(value));
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = buffer.get();
    this.valueSizes[0] = Utils.SIZEOF_DOUBLE;
    return true;
  }
}
