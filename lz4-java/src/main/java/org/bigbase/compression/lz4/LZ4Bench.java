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
package org.bigbase.compression.lz4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class LZ4Bench {

  private static final Logger log = LogManager.getLogger(LZ4Bench.class);

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    String fileName = args[0];

    File f = new File(fileName);
    log.debug("Testing " + args[0]);
    int fileSize = (int) f.length();

    ByteBuffer src = ByteBuffer.allocateDirect(fileSize);
    src.order(ByteOrder.nativeOrder());
    ByteBuffer dst = ByteBuffer.allocateDirect(fileSize);
    dst.order(ByteOrder.nativeOrder());

    FileInputStream fis = new FileInputStream(f);
    FileChannel channel = fis.getChannel();
    int read = 0;
    while ((read += channel.read(src)) < fileSize)
      ;

    log.debug("Read " + read + " bytes");

    src.flip();
    long start = System.currentTimeMillis();
    int compressedSize = LZ4.compress(src, dst);
    long end = System.currentTimeMillis();
    log.debug(
        "Original size="
            + fileSize
            + " comp size="
            + compressedSize
            + " Ratio ="
            + ((double) fileSize / compressedSize)
            + " Time="
            + (end - start)
            + "ms");
  }
}
