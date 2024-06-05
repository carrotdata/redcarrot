/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.commands;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.commands.RedisCommand.ReplyType;
import com.carrotdata.redcarrot.redis.lists.Lists;
import com.carrotdata.redcarrot.redis.sets.Sets;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class LPOP implements RedisCommand {

  /** TODO: 6.2 COUNT support */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int count = 1;
      boolean countSet = false;

      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 2
      /** && numArgs != 3 */
      ) {
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
      // if (numArgs == 3) {
      // int countSize = UnsafeAccess.toInt(inDataPtr);
      // inDataPtr += Utils.SIZEOF_INT;
      // long countPtr = inDataPtr;
      // count = (int) Utils.strToLong(countPtr, countSize);
      // countSet = true;
      // }

      int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
      ;

      // FIXME: We always return ARRAY - this is not original spec
      int size = (int) Lists.LPOP(map, keyPtr, keySize, outBufferPtr + off, outBufferSize - off);
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size);
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }
}
