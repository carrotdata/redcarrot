package org.bigbase.carrot;

import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.util.RangeTree;
import org.bigbase.carrot.util.UnsafeAccess;

import java.util.Objects;

public class OrphanMemoryStats {
  private final String codecName;
  private final String testName;
  private final String debug;
  private final RangeTree allocMap;

  public OrphanMemoryStats(
      Codec codec, String testName, boolean debug, UnsafeAccess.MallocStats mallocStats) {
    this.codecName = Objects.isNull(codec) ? "none" : codec.getType().name();
    this.testName = testName.substring(0, testName.indexOf("["));
    this.debug = String.valueOf(debug);
    this.allocMap = new RangeTree();
    mallocStats.getAllocMap().entrySet().stream()
        // filtered out buffers
        .filter(
            entry ->
                mallocStats.isStackTraceRecordingEnabled()
                    && notBuffer(entry.getKey().getStart(), entry.getKey().getStart(), mallocStats))
        .forEach(
            entry ->
                this.allocMap.add(
                    new RangeTree.Range(entry.getKey().getStart(), entry.getKey().getSize())));
  }

  private boolean notBuffer(long start, long size, UnsafeAccess.MallocStats mallocStats) {
    return !CarrotCoreBase2.bufferSizes.contains((int) size)
        && !(mallocStats.isStackTraceRecordingEnabled()
            && Objects.nonNull(mallocStats.getStackTraceMap().get(start)));
  }

  public String getCodecName() {
    return codecName;
  }

  public String getTestName() {
    return testName;
  }

  public String getDebug() {
    return debug;
  }

  public int getOrphCounter() {
    return allocMap.size();
  }

  public RangeTree getAllocMap() {
    return allocMap;
  }

  @Override
  public String toString() {
    return "OrphanMemoryStats{"
        + "codecName='"
        + codecName
        + '\''
        + ", testName='"
        + testName
        + '\''
        + ", debug='"
        + debug
        + '\''
        + ", allocMap="
        + allocMap.size()
        + '}';
  }
}
