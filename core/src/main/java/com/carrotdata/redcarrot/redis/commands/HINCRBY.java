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
package com.carrotdata.redcarrot.redis.commands;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.redis.hashes.Hashes;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class HINCRBY implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 4) {
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
    // read field
    int fieldSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long fieldPtr = inDataPtr;
    inDataPtr += fieldSize;
    int incrSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;

    try {
      long incrValue = Utils.strToLong(inDataPtr, incrSize);
      long newValue = Hashes.HINCRBY(map, keyPtr, keySize, fieldPtr, fieldSize, incrValue);
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INTEGER.ordinal());
      UnsafeAccess.putLong(outBufferPtr + Utils.SIZEOF_BYTE, newValue);
    } catch (OperationFailedException e) {
      Errors.write(
          outBufferPtr,
          Errors.TYPE_GENERIC,
          Errors.ERR_WRONG_NUMBER_FORMAT,
          ": " + new String(Errors.ERR_KEY_NOT_NUMBER));
    } catch (NumberFormatException e) {
      Errors.write(
          outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, ": " + e.getMessage());
    }
  }
}
