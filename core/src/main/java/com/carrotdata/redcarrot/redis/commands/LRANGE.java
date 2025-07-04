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
import com.carrotdata.redcarrot.redis.lists.Lists;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class LRANGE implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 4) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      inDataPtr = skip(inDataPtr, 1);

      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;

      long start = 0;
      long end = 0;
      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      start = Utils.strToLong(valPtr, valSize);

      inDataPtr += valSize;
      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      valPtr = inDataPtr;
      end = Utils.strToLong(valPtr, valSize);

      long size = Lists.LRANGE(map, keyPtr, keySize, start, end,
        outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
        outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);

      // VARRAY reply
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.VARRAY.ordinal());
      if (size > outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT) {
        size += Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
      }
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, (int) size);

    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT,
        ": " + e.getMessage());
    }
  }
}
