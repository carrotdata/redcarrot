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
package com.carrotdata.redcarrot.redis.strings;

import com.carrotdata.redcarrot.DataBlock;
import com.carrotdata.redcarrot.ops.Operation;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

/**
 * String value length operation. Returns the length of the string value stored at key. An error is
 * returned when key holds a non-string value.
 */
public class StringLength extends Operation {

  int strlen = 0;

  public StringLength() {
    setReadOnly(true);
  }

  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }

    int valueSize = DataBlock.valueLength(foundRecordAddress);
    this.strlen = valueSize;
    return true;
  }

  @Override
  public void reset() {
    super.reset();
    this.strlen = 0;
    setReadOnly(true);
  }

  /**
   * Returns string value
   *
   * @return value length or 0 , if not found
   */
  public int getLength() {
    return this.strlen;
  }
}
