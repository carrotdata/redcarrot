package org.bigbase.carrot;

import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

@RunWith(Parameterized.class)
public class CarrotCoreBase2 {

    protected static Codec codec;
    protected static boolean memoryDebug;

    public CarrotCoreBase2(Object c, Object m) throws IOException {
        codec = (Codec) c;
        memoryDebug = (boolean) m;
    }

    @Parameterized.Parameters(name = "Run with codec={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][]{
                        {CodecFactory.getInstance().getCodec(CodecType.NONE), false},
                        {CodecFactory.getInstance().getCodec(CodecType.NONE), true},
                        {CodecFactory.getInstance().getCodec(CodecType.LZ4), false},
                        {CodecFactory.getInstance().getCodec(CodecType.LZ4), true}
                        //          {CodecFactory.getInstance().getCodec(CodecType.LZ4HC)}
                });
    }

    protected String getParameters() {
        return String.format(
                " with parameters: ['codec': '%s', memory debug: '%s']",
                Objects.nonNull(codec) ? codec.getType().name() : "none", memoryDebug);
    }
}
