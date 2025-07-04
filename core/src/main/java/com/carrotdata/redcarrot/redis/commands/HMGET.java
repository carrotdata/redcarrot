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

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.hashes.Hashes;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class HMGET implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);

    if (numArgs < 3) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    // Skip command name
    inDataPtr = skip(inDataPtr, 1);
    int keySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long keyPtr = inDataPtr;
    inDataPtr += keySize;

    long[] fieldPtrs = Utils.loadPointers(inDataPtr, numArgs - 2);
    int[] fieldSizes = Utils.loadSizes(inDataPtr, numArgs - 2);

    int size = (int) Hashes.HMGET(map, keyPtr, keySize, fieldPtrs, fieldSizes,
      outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
      outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);

    // Array reply
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ARRAY.ordinal());
    // Array serialized size
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE,
      size + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);
    // All data is in the out buffer, including number of elements
  }
}
