package org.bigbase.carrot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public abstract class CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(CarrotCoreBase.class);

  public static final List<Integer> bufferSizes = Arrays.asList(4096, 64);

  private static final long MEM_ALLOCATE = 1000000000L;

  private static final long KEY_VALUE_SIZE = 100000L;
  private static final long KEY_VALUE_SIZE_DEBUG = 1000L;

  protected Codec codec;
  protected static boolean memoryDebug;

  protected BigSortedMap map;
  protected long nKeyValues;

  @Rule public TestName testName = new TestName();

  @BeforeClass
  public static void beforeClass() {}

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
    if (memoryDebug) {
      UnsafeAccess.setMallocDebugStackTraceEnabled(true);
      UnsafeAccess.setStackTraceRecordingFilter(bufferSizes::contains);
    }
  }

  @After
  public void tearDown() {
    log.debug("tearDown: {}", testName.getMethodName());

    if (Objects.isNull(map)) return;
    // Dispose
    map.dispose();

    // implement to free test specific stuff(could be empty).
    extTearDown();

    if (memoryDebug) {
      UnsafeAccess.mallocStats.printStats(testName.getMethodName());
      BigSortedMap.printGlobalMemoryAllocationStats();
    }
  }

  public abstract void extTearDown();

  @Parameterized.Parameters(name = "Run with codec={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {CodecFactory.getInstance().getCodec(CodecType.LZ4)},
          {CodecFactory.getInstance().getCodec(CodecType.NONE)}
          //          {CodecFactory.getInstance().getCodec(CodecType.LZ4HC)}
        });
  }

  /** @return Tests parameters */
  protected String getTestParameters() {
    return String.format("Test: %s", testName.getMethodName());
  }
}
