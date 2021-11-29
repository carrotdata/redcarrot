package org.bigbase.carrot;

import org.bigbase.carrot.compression.Codec;

import java.util.Objects;

public class MemoryStats {
  private final String codecName;
  private final String testName;
  private final boolean debug;
  private final int orphCounter;

  public MemoryStats(Codec codec, String testName, boolean debug, int orphCounter) {
    this.codecName = Objects.isNull(codec) ? "none" : codec.getType().name();
    this.testName = testName.substring(0, testName.indexOf("["));
    this.debug = debug;
    this.orphCounter = orphCounter;
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
