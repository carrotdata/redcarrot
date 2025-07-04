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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.compression.Codec;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;

@RunWith(Parameterized.class)
public abstract class CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(CarrotCoreBase.class);

  public static final List<Integer> bufferSizes = Arrays.asList(13);

  protected static long MEM_ALLOCATE = 100000000L;

  private static long KEY_VALUE_SIZE = 100000L;
  private static long KEY_VALUE_SIZE_DEBUG = 1000L;

  protected Codec codec;
  protected static boolean memoryDebug;

  protected BigSortedMap map;
  protected long nKeyValues;

  @Rule
  public TestName testName = new TestName();

  static {
    memoryDebug = Boolean.parseBoolean(System.getProperty("memoryDebug"));
    UnsafeAccess.setMallocDebugEnabled(memoryDebug);
  }

  /** @param c - Codec, null - no codec, LZ4... - other mem allocation */
  public CarrotCoreBase(Object c) {
    codec = (Codec) c;
    BigSortedMap.setCompressionCodec(codec);
  }

  /*
   * Subclasses can override below setUp and tearDown, but MUST call super. first
   */
  public void setUp() throws IOException {
    log.debug("SetUp {} ", getTestParameters());

    map = new BigSortedMap(MEM_ALLOCATE);
    nKeyValues = memoryDebug ? KEY_VALUE_SIZE_DEBUG : KEY_VALUE_SIZE;
  }

  @After
  public void tearDown() {
    log.debug("tearDown: {}", getTestParameters()); // getTestParameters());

    if (Objects.isNull(map)) return;
    // Dispose
    map.dispose();

    // implement to free test specific stuff(could be empty).
    extTearDown();

    if (memoryDebug) {
      UnsafeAccess.mallocStats.printStats(getTestParameters()); // getTestParameters());
      BigSortedMap.printGlobalMemoryAllocationStats();
    }
  }

  public abstract void extTearDown();

  @Parameterized.Parameters(name = "Run with codec={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { CodecFactory.getInstance().getCodec(CodecType.LZ4) },
      { CodecFactory.getInstance().getCodec(CodecType.ZSTD) },
        { CodecFactory.getInstance().getCodec(CodecType.NONE) }
        // {CodecFactory.getInstance().getCodec(CodecType.LZ4HC)}
    });
  }

  /** @return Tests parameters */
  protected String getTestParameters() {
    return String.format("%s.%s(codec=%s)",
      getClass().getName().substring(getClass().getName().lastIndexOf(".") + 1),
      testName.getMethodName().substring(0, testName.getMethodName().indexOf("[")),
      testName.getMethodName().contains("codec=null") ? "null"
          : testName.getMethodName().substring(testName.getMethodName().lastIndexOf(".") + 1,
            testName.getMethodName().length() - 15));
  }
}
