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
package com.carrotdata.redcarrot.redis.hashes;

import static com.carrotdata.redcarrot.redis.util.Commons.elementAddressFromKey;
import static com.carrotdata.redcarrot.redis.util.Commons.elementSizeFromKey;
import static com.carrotdata.redcarrot.redis.util.Commons.keySizeWithPrefix;

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.ops.Operation;
import com.carrotdata.redcarrot.util.Utils;

public class HashValueLength extends Operation {

  private int foundValueSize = 0;

  public HashValueLength() {
    setFloorKey(true);
    setReadOnly(true);
  }

  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    foundValueSize = 0;
    setReadOnly(true);
  }

  public int getFoundValueSize() {
    return foundValueSize;
  }

  @Override
  public boolean execute() {
    if (foundRecordAddress <= 0) {
      return false;
    }
    // check prefix
    int setKeySize = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySize) {
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySize, foundKeyAddress, setKeySize) != 0) {
      return false;
    }

    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
    // Set no updates
    updatesCount = 0;
    long address = Hashes.exactSearch(foundRecordAddress, fieldPtr, fieldSize);
    if (address < 0) {
      return false;
    }
    // size of a field-value pair
    foundValueSize = Hashes.getValueSize(address);
    return true;
  }
}
