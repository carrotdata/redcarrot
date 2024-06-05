/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot;

import java.util.ArrayList;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.util.Bytes;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;

public class IndexBlockScannerLargeKVsTest extends IndexBlockScannerTest {

  private static final Logger log = LogManager.getLogger(IndexBlockScannerLargeKVsTest.class);

  protected ArrayList<Key> fillIndexBlock(IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("FILL seed={}", seed);
    int maxSize = 2048;
    while (true) {
      int len = r.nextInt(maxSize - 2) + 2;
      byte[] key = new byte[len];
      r.nextBytes(key);
      key = Bytes.toHex(key).getBytes();
      len = key.length;
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, ptr, len);
      if (b.put(ptr, len, ptr, len, 0, 0)) {
        keys.add(new Key(ptr, len));
      } else {
        UnsafeAccess.free(ptr);
        break;
      }
    }
    log.debug("Number of data blocks={} index block data size ={} num records={}",
      b.getNumberOfDataBlock(), b.getDataInBlockSize(), keys.size());
    return keys;
  }
}
