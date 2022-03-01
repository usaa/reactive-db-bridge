package com.usaa.reactive.r2dbc.db2.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

/**
 * Helper functions to help bridge Vert.X API's to Reactor types
 */
@Slf4j
public class HandlerSupport {
    public static <T> Mono<T> asMono(Consumer<Handler<AsyncResult<T>>> doThis) {
        return Mono.defer(() -> Mono.create(sink -> doThis.accept(toHandler(sink))));
    }

    /**
     * Creates a Vert.X Handler that will feed success/error signals into the given MonoSink
     * @param <T> The value type that will be produced to the Handler
     * @param sink A MonoSink to which success or error signals will be propagated
     * @return A Handler suitable to pass into an asynchronous Vert.X method
     */
    private static <T> Handler<AsyncResult<T>> toHandler(MonoSink<T> sink) {
        return asyncResult -> {
            if (asyncResult.succeeded()) {
                T result = asyncResult.result();
                if (result == null) {
                    sink.success();
                } else {
                    sink.success(result);
                }
            } else {
                sink.error(asyncResult.cause());
            }
        };
    }




}