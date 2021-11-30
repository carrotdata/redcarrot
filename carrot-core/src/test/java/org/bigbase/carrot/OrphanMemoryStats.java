package org.bigbase.carrot;

import org.bigbase.carrot.compression.Codec;

import java.util.Objects;

public class OrphanMemoryStats {
  private final String codecName;
  private final String testName;
  private final String debug;
  private final int orphCounter;

  public OrphanMemoryStats(Codec codec, String testName, boolean debug, int orphCounter) {
    this.codecName = Objects.isNull(codec) ? "none" : codec.getType().name();
    this.testName = testName.substring(0, testName.indexOf("["));
    this.debug = String.valueOf(debug);
    this.orphCounter = orphCounter;
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
    return orphCounter;
  }

  @Override
  public String toString() {
    return "MemoryStats{"
        + "codecName='"
        + codecName
        + '\''
        + ", testName='"
        + testName
        + '\''
        + ", debug="
        + debug
        + ", orphCounter="
        + orphCounter
        + '}';
  }
}
