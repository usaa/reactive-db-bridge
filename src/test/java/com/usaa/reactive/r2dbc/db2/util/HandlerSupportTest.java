package com.usaa.reactive.r2dbc.db2.util;

import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.subscriber.TestSubscriber;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HandlerSupportTest {
    @Test
    public void testEmpty() {
        Promise<Void> promise = Promise.promise();
        Mono<Void> mono = HandlerSupport.asMono(h -> promise.future().onComplete(h));
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        mono.subscribe(subscriber);
        assertFalse(subscriber.isTerminatedOrCancelled(), "Subscriber shouldn't have terminated yet");
        promise.complete();
        assertTrue(subscriber.isTerminatedComplete(), "Subscriber should be terminated complete");
        assertThat("Subsriber shouldn't have received any items",
                subscriber.getReceivedOnNext(), empty());
        assertThat("Mono violated Reactive Streams protocol",
                subscriber.getProtocolErrors(), empty());
    }

    @Test
    public void testSuccess() {
        Promise<String> promise = Promise.promise();
        Mono<String> mono = HandlerSupport.asMono(h -> promise.future().onComplete(h));
        TestSubscriber<String> subscriber = TestSubscriber.create();
        mono.subscribe(subscriber);
        assertFalse(subscriber.isTerminatedOrCancelled(), "Subscriber shouldn't have terminated yet");
        promise.complete("Hello");
        assertTrue(subscriber.isTerminatedComplete(), "Subscriber should be terminated complete");
        assertThat("Subscriber should have received the item",
                subscriber.getReceivedOnNext(), contains("Hello"));
        assertThat("Mono violated Reactive Streams protocol",
                subscriber.getProtocolErrors(), empty());
    }

    @Test
    public void testError() {
        Promise<Void> promise = Promise.promise();
        Mono<Void> mono = HandlerSupport.asMono(h -> promise.future().onComplete(h));
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        mono.subscribe(subscriber);
        assertFalse(subscriber.isTerminatedOrCancelled(), "Subscriber shouldn't have terminated yet");
        promise.fail(new NegativeArraySizeException("This is a pretty random-looking exception class, isn't it?"));
        assertThat("Subscriber should have terminated with the desired error",
                subscriber.expectTerminalError(), instanceOf(NegativeArraySizeException.class));
        assertThat("Subsriber shouldn't have received any items",
                subscriber.getReceivedOnNext(), empty());
        assertThat("Mono violated Reactive Streams protocol",
                subscriber.getProtocolErrors(), empty());
    }
}
