package org.bigbase.carrot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

@RunWith(Parameterized.class)
public class CarrotCoreBase2 {

    private static final Logger log = LogManager.getLogger(CarrotCoreBase2.class);

    protected static Codec codec;
    protected final static boolean memoryDebug = true;

    protected static BigSortedMap map;
    protected static long nKeyValues = 0L;

    /**
     * Alloc 1GB or 10x less for debug
     */
    protected static final long MEM_ALLOCATE = 1000000000L;
    protected static final long MEM_ALLOCATE_DEBUG = 100000L;

    protected static final long KEY_VALUE_SIZE = 100000L;
    protected static final long KEY_VALUE_SIZE_DEBUG = 1000L;

    /**
     * @param c - Codec, null - no codec, LZ4... - other mem allocation
     */
    public CarrotCoreBase2(Object c) {
        codec = (Codec) c;
    }

    @Parameterized.Parameters(name = "Run with codec={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][]{
                        {CodecFactory.getInstance().getCodec(CodecType.LZ4)},
                        {CodecFactory.getInstance().getCodec(CodecType.NONE)}
//                        {CodecFactory.getInstance().getCodec(CodecType.NONE), true},
//                        {CodecFactory.getInstance().getCodec(CodecType.NONE), false}
                        //          {CodecFactory.getInstance().getCodec(CodecType.LZ4HC)}
                });
    }

    /**
     * @return Tests parameters
     */
    protected String getParameters() {
        return String.format(
                " with parameters: ['codec': '%s', memory debug: '%s']",
                Objects.nonNull(codec) ? codec.getType().name() : "none", memoryDebug);
    }
}
