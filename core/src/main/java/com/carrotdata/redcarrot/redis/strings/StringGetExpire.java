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
import com.carrotdata.redcarrot.util.Utils;

/** String GETEXPIRE operation. (Utility helper) */
public class StringGetExpire extends Operation {

  public StringGetExpire() {
    setReadOnly(true);
  }

  @Override
  public boolean execute() {

    if (foundRecordAddress > 0) {
      this.expire = DataBlock.getRecordExpire(foundRecordAddress);
    } else {
      // Does not exist
      this.updatesCount = 0;
      return false;
    }
    return true;
  }

  @Override
  public void reset() {
    super.reset();
    setReadOnly(true);
  }
}
