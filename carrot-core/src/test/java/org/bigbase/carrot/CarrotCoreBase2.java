package org.bigbase.carrot;

import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public abstract class CarrotCoreBase2 {

  private static final long MEM_ALLOCATE = 1000000000L;
  private static final long MEM_ALLOCATE_DEBUG = 100000L;

  private static final long KEY_VALUE_SIZE = 100000L;
  private static final long KEY_VALUE_SIZE_DEBUG = 1000L;

  protected Codec codec;
  protected boolean memoryDebug;

  protected BigSortedMap map;
  protected long nKeyValues;

  @Rule public TestName testName = new TestName();

  protected static final List<MemoryStats> memoryStatsList = new ArrayList<>();

  /** @param c - Codec, null - no codec, LZ4... - other mem allocation */
  public CarrotCoreBase2(Object c, Object m) {
    codec = (Codec) c;
    BigSortedMap.setCompressionCodec(codec);
    memoryDebug = (boolean) m;

    UnsafeAccess.setMallocDebugEnabled(memoryDebug);
    UnsafeAccess.setMallocDebugStackTraceEnabled(memoryDebug);
    if (memoryDebug)
      UnsafeAccess.setStackTraceRecordingFilter(x -> x == MEM_ALLOCATE_DEBUG || x == 2001);

    map = new BigSortedMap(memoryDebug ? MEM_ALLOCATE_DEBUG : MEM_ALLOCATE);
    nKeyValues = memoryDebug ? KEY_VALUE_SIZE_DEBUG : KEY_VALUE_SIZE;

    setUp();
  }

  public abstract void setUp();

  @After
  public abstract void tearDown();

  @Parameterized.Parameters(name = "Run with codec={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {CodecFactory.getInstance().getCodec(CodecType.LZ4), true},
          {CodecFactory.getInstance().getCodec(CodecType.LZ4), false},
          {CodecFactory.getInstance().getCodec(CodecType.NONE), true},
          {CodecFactory.getInstance().getCodec(CodecType.NONE), false}
          //          {CodecFactory.getInstance().getCodec(CodecType.LZ4HC)}
        });
  }

  /** @return Tests parameters */
  protected String getParameters() {
    return String.format(
        " with parameters: ['codec': '%s', memory debug: '%s']",
        Objects.nonNull(codec) ? codec.getType().name() : "none", memoryDebug);
  }
}
