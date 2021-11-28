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
    protected static boolean memoryDebug;

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
     * @param m true - in test mode, false - rea
     */
    public CarrotCoreBase2(Object c, Object m) {
        codec = (Codec) c;
        memoryDebug = (boolean) m;
    }

    @Parameterized.Parameters(name = "Run with codec={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][]{
                        {CodecFactory.getInstance().getCodec(CodecType.LZ4), true},
//                        {CodecFactory.getInstance().getCodec(CodecType.NONE), true},
                        {CodecFactory.getInstance().getCodec(CodecType.LZ4), false}
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

    /**
     * Allocate memory for a test
     */
    protected static void setUp() {
        map = new BigSortedMap(memoryDebug ? MEM_ALLOCATE_DEBUG : MEM_ALLOCATE);
        nKeyValues = memoryDebug ? KEY_VALUE_SIZE_DEBUG : KEY_VALUE_SIZE;
        log.debug("Allocated memory: {} bytes with key-value size: {}",
                memoryDebug ? MEM_ALLOCATE_DEBUG : MEM_ALLOCATE, nKeyValues);
    }

    /**
     * @return true - memory deallocated, false - don't
     */
    protected static boolean isDisposed() {
        if (Objects.isNull(map)) return false;
        map.dispose();
        log.debug("globalDataSize : {}", BigSortedMap.getGlobalDataSize());
        return BigSortedMap.getGlobalAllocatedMemory() == 0L;
    }
}
