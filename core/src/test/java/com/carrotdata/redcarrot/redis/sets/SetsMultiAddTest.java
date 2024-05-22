package com.carrotdata.redcarrot.redis.sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.util.Bytes;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Value;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SetsMultiAddTest {

    private static final Logger log = LogManager.getLogger(SetsTest.class);

    BigSortedMap map;
    Key key;
    long buffer;
    int valSize = 16;
    List<Value> values;


    @Test
    public void testMultiAdd() {
        log.debug("Test multi add");

        map = new BigSortedMap(1000000000L);
        values = getRandomValues(1000L);

        List<Value> copy = new ArrayList<Value>();
        values.stream().forEach(x -> copy.add(x));

        Key key = getKey();
        int num = Sets.SADD(map, key.address, key.length, copy);
        assertEquals(values.size(), num);
        assertEquals(values.size(), (int) Sets.SCARD(map, key.address, key.length));
        for (Value v : values) {
            int result = Sets.SISMEMBER(map, key.address, key.length, v.address, v.length);
            assertEquals(1, result);
        }
        List<byte[]> members = Sets.SMEMBERS(map, key.address, key.length, values.size() * valSize * 2);
        assertNotNull(members);
        for (byte[] v : members) {
            log.debug("{}", Bytes.toHex(v));
        }
        tearDown();
    }

    private List<Value> getRandomValues(long n) {
        List<Value> values = new ArrayList<Value>();
        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        log.debug("VALUES SEED={}", seed);
        byte[] buf = new byte[valSize];
        for (int i = 0; i < n; i++) {
            r.nextBytes(buf);
            long ptr = UnsafeAccess.malloc(valSize);
            UnsafeAccess.copy(buf, 0, ptr, buf.length);
            values.add(new Value(ptr, valSize));
        }
        return values;
    }

    private Key getKey() {
        long ptr = UnsafeAccess.malloc(valSize);
        byte[] buf = new byte[valSize];
        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        log.debug("KEY SEED={}", seed);
        r.nextBytes(buf);
        UnsafeAccess.copy(buf, 0, ptr, valSize);
        return key = new Key(ptr, valSize);
    }

    public void tearDown() {
        if (Objects.isNull(map)) return;

        // Dispose
        map.dispose();
        if (key != null) {
            UnsafeAccess.free(key.address);
            key = null;
        }
        for (Value v : values) {
            UnsafeAccess.free(v.address);
        }
        if (buffer > 0) {
            UnsafeAccess.free(buffer);
            buffer = 0;
        }
    }
}
