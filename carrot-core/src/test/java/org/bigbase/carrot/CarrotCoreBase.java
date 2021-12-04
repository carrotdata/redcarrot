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
  protected boolean memoryDebug;

  protected BigSortedMap map;
  protected long nKeyValues;

  @Rule public TestName testName = new TestName();

  protected static final List<OrphanMemoryStats> orphanMemoryStatsList = new ArrayList<>();

  @BeforeClass
  public static void beforeClass() {
    orphanMemoryStatsList.clear();
  }

  /** @param c - Codec, null - no codec, LZ4... - other mem allocation */
  public CarrotCoreBase(Object c, Object m) {
    codec = (Codec) c;
    BigSortedMap.setCompressionCodec(codec);
    memoryDebug = (boolean) m;

    UnsafeAccess.setMallocDebugEnabled(memoryDebug);
  }

  /*
   * Subclasses can override below setUp and tearDown, but MUST call super. first
   */
  @Before
  public void setUp() throws IOException {
    log.debug("SetUp {} ", getTestParameters());

    map = new BigSortedMap(MEM_ALLOCATE);
    nKeyValues = memoryDebug ? KEY_VALUE_SIZE_DEBUG : KEY_VALUE_SIZE;
    if (Boolean.parseBoolean(System.getProperty("memoryDebug"))) {
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

    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.printGlobalMemoryAllocationStats();

    orphanMemoryStatsList.add(
        new OrphanMemoryStats(
            codec, testName.getMethodName(), memoryDebug, UnsafeAccess.mallocStats));
  }

  public abstract void extTearDown();

  @Parameterized.Parameters(name = "Run with codec={0} memory debug={1}")
  public static Collection<Object[]> data() {
    if (Boolean.parseBoolean(System.getProperty("memoryDebug"))) {
      return Arrays.asList(
          new Object[][] {
            {CodecFactory.getInstance().getCodec(CodecType.LZ4), true},
            {CodecFactory.getInstance().getCodec(CodecType.LZ4), false},
            {CodecFactory.getInstance().getCodec(CodecType.NONE), true},
            {CodecFactory.getInstance().getCodec(CodecType.NONE), false}
            //          {CodecFactory.getInstance().getCodec(CodecType.LZ4HC)}
          });
    } else {
      return Arrays.asList(
          new Object[][] {{CodecFactory.getInstance().getCodec(CodecType.NONE), false}});
    }
  }

  /** @return Tests parameters */
  protected String getTestParameters() {
    return String.format("Test: %s", testName.getMethodName());
  }

  /**
   * FIXME: Not sure how do you calculate mismatch?
   *
   * <p>1. On each teardown method test create on object in the orphanMemoryStatsList:
   * OrphanMemoryStats .
   *
   * <p>2.On printTestStatistics method groupBy all results from the list by: ms.getTestName() and
   * ms.getDebug() for each pair created list for other parameters. In our case codec. print each
   * pair filter where OrphCounter not equal.
   *
   * <p>for example: MemoryStats{codecName='LZ4', testName='testSetGetBit', debug=true,
   * orphCounter=8}, MemoryStats{codecName='none', testName='testSetGetBit', debug=true,
   * orphCounter=9 for test: testSetGetBit, debug mode, codec: 'LZ4' has 8 orphan objects but codec:
   * 'None' has 9
   *
   * <p>Can you just emit orphanCounts for every test in debug mode? This method will print objects
   * if they different from one test to other. There is one line only will be printed if
   * orphanCounts are equals which is: "CarrotCoreBase.finalTearDown; Number of tests with objects
   * leaks: 0"
   */
  @AfterClass
  public static void printTestStatistics() {
    log.debug(
        "CarrotCoreBase.finalTearDown; Number of objects with memory leaks is: {}",
        orphanMemoryStatsList.stream()
            .collect(
                Collectors.groupingBy(
                    CarrotCoreBase::getGroupingByKey,
                    Collectors.mapping((OrphanMemoryStats ms) -> ms, Collectors.toList())))
            .entrySet()
            .stream()
            .filter(set -> isOrphNotEquals(set.getValue()))
            .peek(ms -> log.debug("Orphan objects mismatch: {}", ms))
            .flatMap(ms -> ms.getValue().stream())
            .peek(oms -> log.debug("{}", oms))
            .flatMap(oms -> oms.getAllocMap().entrySet().stream())
            .peek(
                entry ->
                    log.debug(
                        "Memory start:{} size: {}",
                        entry.getKey().getStart(),
                        entry.getKey().getSize()))
            .count());
  }

  private static boolean isOrphNotEquals(List<OrphanMemoryStats> msList) {
    return msList.stream().anyMatch(ms -> ms.getOrphCounter() != msList.get(0).getOrphCounter());
  }

  private static String getGroupingByKey(OrphanMemoryStats ms) {
    return ms.getTestName() + "-" + ms.getDebug();
  }
}
