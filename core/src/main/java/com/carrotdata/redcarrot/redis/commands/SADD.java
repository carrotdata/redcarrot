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
import com.carrotdata.redcarrot.redis.sets.Sets;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;
import com.carrotdata.redcarrot.util.Value;

public class SADD implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs <= 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    // skip command name
    inDataPtr = skip(inDataPtr, 1);
    // read key
    int keySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long keyPtr = inDataPtr;
    inDataPtr += keySize;

    // long[] ptrs = Utils.loadPointers(inDataPtr, numArgs - 2);
    // int[] sizes = Utils.loadSizes(inDataPtr, numArgs - 2);
    //
    // int num = Sets.SADD(map, keyPtr, keySize, ptrs, sizes);
    //
    List<Value> kvs = Utils.loadValues(inDataPtr, numArgs - 2);
    int num = Sets.SADD(map, keyPtr, keySize, Utils.copyValues(kvs));
    // INT REPLY
    INT_REPLY(outBufferPtr, num);
  }
}
