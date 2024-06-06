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
package com.carrotdata.redcarrot;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class DataBlockTestBase {

  private static final Logger log = LogManager.getLogger(DataBlockTestBase.class);

  protected DataBlock getDataBlock() {
    IndexBlock ib = new IndexBlock(null, 4096);
    ib.setFirstIndexBlock();
    ib.firstBlock();
    return ib.firstBlock();
  }

  protected boolean contains(long key, int keySize, List<Key> keys) {
    for (Key k : keys) {
      if (Utils.compareTo(k.address, k.length, key, keySize) == 0) {
        return true;
      }
    }
    return false;
  }

  protected boolean isValidFailure(DataBlock b, Key key, int valLen, int oldValLen) {
    int dataSize = b.getDataInBlockSize();
    int blockSize = b.getBlockSize();
    int newRecSize = key.length + valLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(key.length, valLen)) {
      newRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    int oldRecSize = key.length + oldValLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(key.length, oldValLen)) {
      oldRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }

    return dataSize + newRecSize - oldRecSize > blockSize;
  }

  protected void scanAndVerify(DataBlock b) throws RetryOperationException, IOException {
    long buffer;
    long tmp = 0;

    DataBlockScanner bs = DataBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
    int prevKeySize = 0;
    int count = 0;
    assertNotNull(bs);
    while (bs.hasNext()) {
      int keySize = bs.keySize();
      buffer = UnsafeAccess.malloc(keySize);
      bs.key(buffer, keySize);
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, prevKeySize, buffer, keySize);
        assertTrue(res < 0);
        UnsafeAccess.free(tmp);
      }
      tmp = buffer;
      prevKeySize = keySize;
    }
    UnsafeAccess.free(tmp);
    bs.close();
    log.debug("Scanned {}", count);
  }
}
