/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.compression;

// TODO: Auto-generated Javadoc

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.nio.ByteBuffer;

/** A factory for creating Codec objects. */
public class CodecFactory {

  private static Logger logger = LogManager.getLogger(CodecFactory.class);
  /** The factory instance. */
  private static CodecFactory sInstance;

  /** Boolean array with TRUE value for each codec if supported, FALSE otherwise */
  private static boolean[] supportedCodecs = new boolean[CodecType.values().length];

  private static Codec[] codecs = { new LZ4Codec(), new LZ4HCCodec(), null, new ZstdCodec() };

  /**
   * Gets the single instance of CodecFactory.
   * @return single instance of CodecFactory
   */
  public static synchronized CodecFactory getInstance() {
    if (sInstance == null) {
      sInstance = new CodecFactory();
    }
    return sInstance;
  }

  private CodecFactory() {
    // check codec availability

    // test buffer to compress
    ByteBuffer buf = ByteBuffer.allocateDirect(256);
    for (int i = 0; i < 256; i++) {
      buf.put((byte) i);
    }

    // test each codec and memorize if it is supported
    for (Codec codec : codecs) {
      if (codec != null) {
        supportedCodecs[codec.getType().ordinal()] = checkCodec(codec, buf);
      }
    }
  }

  private boolean checkCodec(Codec codec, ByteBuffer buf) {
    try {
      buf.rewind();
      ByteBuffer buf2 = ByteBuffer.allocateDirect(512);
      @SuppressWarnings("unused")
      int compressedSize = codec.compress(buf, buf2);
      return true;
    } catch (Throwable ex) {
      logger.warn("Codec " + codec.getType().toString() + " is not supported", ex);
      return false;
    }
  }

  /**
   * Gets the codec.
   * @param type the type
   * @return the codec
   */
  public Codec getCodec(CodecType type) {
    if (!supportedCodecs[type.ordinal()]) {
      return null;
    }
    int id = type.ordinal();
    return getCodec(id);
  }

  /**
   * Gets the codec.
   * @param type the type
   * @return the codec
   */
  public static Codec getCodec(int id) {

    switch (id) {
      case 1:
        return codecs[0];
      case 2:
        return codecs[1];
      case 3:
        return codecs[2];
      case 4:
        return codecs[3];
      // No codec
      case 0:
        return null;
    }
    return null;
  }

  public static Codec[] getCodecs() {
    return codecs;
  }
}
