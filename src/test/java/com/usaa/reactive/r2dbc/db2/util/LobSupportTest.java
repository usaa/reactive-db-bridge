package com.usaa.reactive.r2dbc.db2.util;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LobSupportTest {

    @Test
    public void testCollectLOBs() {
        List<List<Object>> batch = Arrays.asList(
                Arrays.asList(null, 1, "two", Clob.from(Mono.just("three")), Blob.from(Mono.just(ByteBuffer.wrap("four".getBytes())))),
                Arrays.asList(1, "two", Clob.from(Mono.just("three")), Blob.from(Mono.just(ByteBuffer.wrap("four".getBytes()))), null)
        );

        List<List<Object>> result = LobSupport.collectLOBs(batch).collectList().block();
        List<Object> first = result.get(0);
        assertNull(first.get(0), "Null should have been passed through unchanged");
        assertEquals(1, first.get(1), "Integer should have been passed through unchanged");
        assertEquals("two", first.get(2), "String should have been passed through unchanged");
        assertEquals("three", first.get(3), "CLOB should have been converted to String");
        assertArrayEquals("four".getBytes(), (byte[]) first.get(4), "BLOB should have been converted to byte array");

        List<Object> second = result.get(1);
        assertEquals(1, second.get(0), "Integer should have been passed through unchanged");
        assertEquals("two", second.get(1), "String should have been passed through unchanged");
        assertEquals("three", second.get(2), "CLOB should have been converted to String");
        assertArrayEquals("four".getBytes(), (byte[]) second.get(3), "BLOB should have been converted to byte array");
        assertNull(second.get(4), "Null should have been passed through unchanged");
    }
}
