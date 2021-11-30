package org.bigbase.carrot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public abstract class CarrotCoreBase2 {

  private static final Logger log = LogManager.getLogger(CarrotCoreBase2.class);

  private static final long MEM_ALLOCATE = 1000000000L;
  // This is not necessary 
  //private static final long MEM_ALLOCATE_DEBUG = 100000L;

  private static final long KEY_VALUE_SIZE = 100000L;
  private static final long KEY_VALUE_SIZE_DEBUG = 1000L;

  protected Codec codec;
  protected boolean memoryDebug;

  protected BigSortedMap map;
  protected long nKeyValues;

  @Rule public TestName testName = new TestName();

  protected static final List<OrphanMemoryStats> orphanMemoryStatsList = new ArrayList<>();

  /** @param c - Codec, null - no codec, LZ4... - other mem allocation */
  public CarrotCoreBase2(Object c, Object m) {
    codec = (Codec) c;
    BigSortedMap.setCompressionCodec(codec);
    memoryDebug = (boolean) m;

    UnsafeAccess.setMallocDebugEnabled(memoryDebug);
    
    // This should not be set by default
    //UnsafeAccess.setMallocDebugStackTraceEnabled(memoryDebug);
    // This filter is memory leak specific, you can't set it in advance
    //if (memoryDebug)
    //  UnsafeAccess.setStackTraceRecordingFilter(x -> x == MEM_ALLOCATE_DEBUG || x == 2001);
  }

  
  /*
   * Subclasses can override below setUp and tearDown, but MUST call super. first
   */
  @Before
  public void setUp() {
    map = new BigSortedMap(/*memoryDebug ? MEM_ALLOCATE_DEBUG :*/ MEM_ALLOCATE);
    nKeyValues = memoryDebug ? KEY_VALUE_SIZE_DEBUG : KEY_VALUE_SIZE;
  }

  @After
  public void tearDown() {
    if (Objects.isNull(map)) return;
    // Dispose
    map.dispose();
    BigSortedMap.printGlobalMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();
  }

  @Parameterized.Parameters(name = "Run with codec={0} memory debug={1}")
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

  /**
   * FIXME: Not sure how do you calculate mismatch?
   * Can you just emit orphanCounts for every test in debug mode?
   * 
   */
  @AfterClass
  public static void printTestStatistics() {
    log.debug(
        "CarrotCoreBase2.finalTearDown; Number of tests with objects leaks: {}",
        orphanMemoryStatsList.stream()
            .collect(
                Collectors.groupingBy(
                    CarrotCoreBase2::getGroupingByKey,
                    Collectors.mapping((OrphanMemoryStats ms) -> ms, Collectors.toList())))
            .entrySet()
            .stream()
            .filter(set -> isOrphNotEquals(set.getValue()))
            .peek(ms -> log.debug("Orphan objects mismatch: {}", ms))
            .count());
  }

  private static boolean isOrphNotEquals(List<OrphanMemoryStats> msList) {
    return msList.stream().anyMatch(ms -> ms.getOrphCounter() != msList.get(0).getOrphCounter());
  }

  private static String getGroupingByKey(OrphanMemoryStats ms) {
    return ms.getTestName() + "-" + ms.getDebug();
  }
}
