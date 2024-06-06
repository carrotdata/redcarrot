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
package com.carrotdata.redcarrot.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.carrotdata.compression.lz4.LZ4;

// TODO: Auto-generated Javadoc
/**
 * The Class LZ4HCCodec.
 * <p>
 * Compression rates 45-120MB on Intel Core I7 2.2Gh Decompression rates 1000-1800MB
 */
public class LZ4HCCodec implements Codec {

  private static final Logger log = LogManager.getLogger(LZ4HCCodec.class);

  /** The Constant LOG. */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(LZ4HCCodec.class);

  /** The min comp size. */
  private int minCompSize = 100;

  /** The total size. */
  private long totalSize = 0;

  /** The total comp size. */
  private long totalCompSize = 0;

  /** The level. */
  private int level = 1;

  /** Instantiates a new lz4hc codec. */
  public LZ4HCCodec() {
    minCompSize = Integer.parseInt(System.getProperty(COMPRESSION_THRESHOLD, "100"));
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#compress(java.nio.ByteBuffer, java.nio.ByteBuffer)
   */
  @Override
  public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {

    this.totalSize += (src.limit() - src.position());
    int total = LZ4.compressHC(src, dst, 1);
    this.totalCompSize += total;
    return total;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#decompress(java.nio.ByteBuffer, java.nio.ByteBuffer)
   */
  @Override
  public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {

    int total = LZ4.decompressHC(src, dst);
    return total;
  }

  @Override
  public int compress(long src, int srcSize, long dst, int dstCapacity) {
    return LZ4.compressDirectAddressHC(src, srcSize, dst, dstCapacity, level);
  }

  @Override
  public int decompress(long src, int compressedSize, long dst, int dstCapacity) {
    return LZ4.decompressDirectAddressHC(src, compressedSize, dst, dstCapacity);
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getCompressionThreshold()
   */
  @Override
  public int getCompressionThreshold() {

    return minCompSize;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getType()
   */
  @Override
  public CodecType getType() {
    return CodecType.LZ4HC;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#setCompressionThreshold(int)
   */
  @Override
  public void setCompressionThreshold(int val) {
    minCompSize = val;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getAvgCompressionRatio()
   */
  @Override
  public double getAvgCompressionRatio() {
    if (totalCompSize == 0) {
      return 1.d;
    } else {
      return ((double) totalSize) / totalCompSize;
    }
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getLevel()
   */
  @Override
  public int getLevel() {

    return level;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#setLevel(int)
   */
  @Override
  public void setLevel(int level) {
    this.level = level;
  }

  @Override
  public long getTotalBytesProcessed() {
    return totalSize;
  }

  public static void main(String[] args) throws IOException {

    String str =
        "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy";

    str += str;
    str += str;
    str += str;
    str += str;
    str += str;

    ByteBuffer src = ByteBuffer.allocateDirect(102400);
    ByteBuffer dst = ByteBuffer.allocateDirect(102400);
    Codec codec = new LZ4HCCodec();

    byte[] buf = str.getBytes();
    src.put(buf);
    src.flip();
    int compSize = codec.compress(src, dst);
    log.debug("Size={} compressed ={}", str.length(), compSize);

    src.clear();

    int decSize = codec.decompress(dst, src);
    log.debug("Size={} decompressed ={}", str.length(), decSize);
  }
}
