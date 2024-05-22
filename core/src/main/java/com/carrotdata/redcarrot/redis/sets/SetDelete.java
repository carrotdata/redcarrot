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
package com.carrotdata.redcarrot.redis.sets;

import static com.carrotdata.redcarrot.redis.util.Commons.addNumElements;
import static com.carrotdata.redcarrot.redis.util.Commons.elementAddressFromKey;
import static com.carrotdata.redcarrot.redis.util.Commons.elementSizeFromKey;
import static com.carrotdata.redcarrot.redis.util.Commons.isFirstKey;
import static com.carrotdata.redcarrot.redis.util.Commons.keySizeWithPrefix;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.ops.Operation;
import com.carrotdata.redcarrot.redis.util.Commons;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

/**
 * This read-modify-write mutation is executed atomically and isolated It deletes element from a
 * given set, defined by a Key
 */
public class SetDelete extends Operation {
  // TODO: use own keyArena

  BigSortedMap map;
  boolean checkForEmpty;

  public SetDelete() {
    setFloorKey(true);
  }

  public boolean checkForEmpty() {
    return this.checkForEmpty;
  }

  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    this.map = null;
    this.checkForEmpty = false;
  }

  public void setMap(BigSortedMap map) {
    this.map = map;
  }

  @Override
  public boolean execute() {
    if (foundRecordAddress <= 0) {
      return false;
    }
    // check prefix
    int setKeySizeWithPrefix = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySizeWithPrefix) {
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    int setKeySize = setKeySizeWithPrefix - Commons.KEY_PREFIX_SIZE;
    boolean isFirstKey = isFirstKey(foundKeyAddress, foundKeySize, setKeySize);

    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySizeWithPrefix, foundKeyAddress, setKeySizeWithPrefix)
        != 0) {
      return false;
    }

    long elementPtr = elementAddressFromKey(keyAddress);
    int elementSize = elementSizeFromKey(keyAddress, keySize);
    // First two bytes are number of elements in a value
    long addr = Sets.exactSearch(foundRecordAddress, elementPtr, elementSize);
    if (addr < 0) {
      return false;
    }
    // found
    int elemSizeSize = Utils.sizeUVInt(elementSize);
    int toCut = elemSizeSize + elementSize;
    long valueAddress = DataBlock.valueAddress(foundRecordAddress);
    // decrement number of elements in this value
    int numElements = addNumElements(valueAddress, -1);
    if (numElements == 0) {
      this.checkForEmpty = true;
    }
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    int newValueSize = valueSize - toCut;
    Sets.checkValueArena(newValueSize);
    long ptr = Sets.valueArena.get();
    // TODO: check this
    UnsafeAccess.copy(valueAddress, ptr, addr - valueAddress);
    UnsafeAccess.copy(
        addr + toCut, ptr + addr - valueAddress, valueSize - toCut - (addr - valueAddress));

    // set # of updates to 1
    this.updatesCount = 1;
    this.keys[0] = foundKeyAddress;
    this.keySizes[0] = foundKeySize;
    this.values[0] = ptr;
    this.valueSizes[0] = valueSize - toCut;
    if (numElements == 0 && !isFirstKey /*canDelete(foundKeyAddress, foundKeySize)*/) {
      // Delete Key, b/c its empty
      // TODO - this code leaves last key, which needs to be deleted explicitly
      this.updateTypes[0] = true;
    }
    return true;
  }

  /**
   * We can delete K-V only when it is empty, not a first key (ends with '\0' or (TODO) first and
   * the only K-V for the set)
   *
   * @param foundKeyAddress
   * @return true if can be deleted, false -otherwise
   * @throws IOException
   */
  // private boolean canDelete(long foundKeyAddress, int foundKeySize) {
  // if (!firstKVinType(foundKeyAddress, foundKeySize)) {
  // return true;
  // }
  // // this first KV in set, we can delete it if it is the only one in the set
  // return !nextKVisInType(map, foundKeyAddress);
  // }

}
