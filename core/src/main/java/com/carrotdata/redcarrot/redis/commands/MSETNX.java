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
package com.carrotdata.redcarrot.redis.commands;

import java.util.List;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.strings.Strings;
import com.carrotdata.redcarrot.util.KeyValue;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class MSETNX implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs < 3 || (numArgs - 1) % 2 != 0) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    // Skip command name
    inDataPtr = skip(inDataPtr, 1);
    List<KeyValue> kvs = Utils.loadKeyValues(inDataPtr, (numArgs - 1) / 2);
    boolean result = Strings.MSETNX(map, kvs);
    int retValue = 1;
    if (!result) {
      retValue = 0; // operation failed, at kleast one key existed
    }
    INT_REPLY(outBufferPtr, retValue);
  }
}
