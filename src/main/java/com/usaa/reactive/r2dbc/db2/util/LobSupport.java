package com.usaa.reactive.r2dbc.db2.util;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class LobSupport {
    private static final Object NULL_PLACEHOLDER = new Object();

    public static Flux<List<Object>> collectLOBs(List<List<Object>> batch) {
        return Flux.fromIterable(batch)
                // Flux will choke on any null's so we have to replace them with a placeholder for this step
                .flatMap(values -> Flux.fromIterable(replaceNullsWithPlaceholders(values))
                        .flatMap(value -> {
                            if (value instanceof Clob) {
                                Clob clob = (Clob) value;
                                return Flux.from(clob.stream())
                                        .collectList()
                                        .map(list -> String.join("", list))
                                        .cast(Object.class);
                            } else if (value instanceof Blob) {
                                Blob blob = (Blob) value;
                                return Flux.from(blob.stream())
                                        .collectList()
                                        .map(LobSupport::toByteArray)
                                        .cast(Object.class);
                            } else {
                                return Mono.just(value);
                            }
                        })
                        .collectList())
                // We avoided putting true null's in the lists, now we have to put them back
                .map(LobSupport::replacePlaceholdersWithNulls);
    }

    private static Iterable<Object> replaceNullsWithPlaceholders(Iterable<Object> iterable) {
        return () -> {
            Iterator<Object> delegate = iterable.iterator();
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Object next() {
                    Object next = delegate.next();
                    return next == null ? NULL_PLACEHOLDER : next;
                }
            };
        };
    }

    private static List<Object> replacePlaceholdersWithNulls(List<Object> list) {
        return list.stream().map(o -> o == NULL_PLACEHOLDER ? null : o).collect(Collectors.toList());
    }


    private static byte[] toByteArray(List<ByteBuffer> buffers) {
        int totalBytes = buffers.stream().map(Buffer::remaining).reduce(0, Integer::sum);
        byte[] result = new byte[totalBytes];
        int pos = 0;
        for (ByteBuffer buffer : buffers) {
            int remaining = buffer.remaining();
            buffer.get(result, pos, remaining);
            pos += remaining;
        }
        return result;
    }
}
