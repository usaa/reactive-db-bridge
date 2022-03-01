package com.usaa.reactive.r2dbc.db2.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.subscriber.TestSubscriber;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ReactiveSemaphoreTest {
    @Test
    public void testMono() {
        AtomicInteger entryCounter = new AtomicInteger(0);
        Sinks.Empty<Object> waitForThis = Sinks.empty();
        AtomicInteger exitCounter = new AtomicInteger(0);
        CountDownLatch exitLatch = new CountDownLatch(1);
        Mono<Void> protectThisThing = increment(entryCounter)
                .then(waitForThis.asMono())
                .then(increment(exitCounter));
        ReactiveSemaphore semaphore = ReactiveSemaphore.create("testCase");
        Mono<Void> protectedBySemaphore = protectThisThing.transform(semaphore::apply);

        TestSubscriber<Void> subscriber1 = TestSubscriber.builder().initialRequest(0).build();
        TestSubscriber<Void> subscriber2 = TestSubscriber.builder().initialRequest(0).build();

        log.info("Starting subscriber 1");
        protectedBySemaphore.subscribe(subscriber1);
        assertEquals(1, entryCounter.get(), "First subscriber should have entered the protected resource");
        assertEquals(0, exitCounter.get(), "First subscriber should still be waiting");
        assertFalse(subscriber1.isTerminatedOrCancelled(), "First subscriber should still be waiting");

        log.info("Starting subscriber 2");
        protectedBySemaphore.subscribe(subscriber2);
        assertEquals(1, entryCounter.get(), "Second subscriber should be waiting for semaphore");
        assertEquals(0, exitCounter.get(), "First subscriber should still be waiting");
        assertFalse(subscriber1.isTerminatedOrCancelled(), "First subscriber should still be waiting");
        assertFalse(subscriber2.isTerminatedOrCancelled(), "Second subscriber should still be waiting");

        log.info("Allowing subscribers to finish");
        waitForThis.tryEmitEmpty();
        assertEquals(2, entryCounter.get(), "Both subscribers should be complete");
        assertEquals(2, exitCounter.get(), "Both subscribers should be complete");
        assertTrue(subscriber1.isTerminatedOrCancelled(), "First subscriber should be complete");
        assertTrue(subscriber2.isTerminatedOrCancelled(), "Second subscriber should be complete");
    }

    @Test
    public void testMonoReentrant() {
        AtomicInteger entryCounter = new AtomicInteger(0);
        Mono<Void> protectThisThing = increment(entryCounter);
        ReactiveSemaphore semaphore = ReactiveSemaphore.create("testCase");

        Mono<Void> anotherThing = protectThisThing
                .transform(semaphore::apply)
                // Now do some other operations
                .doOnSubscribe(s -> log.debug("Yep, we've subscribed"))
                .switchIfEmpty(Mono.empty())
                .map(v -> v)
                .doOnNext(v -> log.error("This wont happen"))
                // And protect this overall operation with the semaphore (again)
                .transform(semaphore::apply)
                // In case our re-entrant behavior is broken, don't block for long
                .timeout(Duration.ofMillis(500));

        TestSubscriber<Void> subscriber = TestSubscriber.builder().initialRequest(0).build();

        log.info("Starting subscriber");
        anotherThing.subscribe(subscriber);
        assertEquals(1, entryCounter.get(), "Subscriber should have obtained the semaphore and entered");
        assertTrue(subscriber.isTerminatedComplete(), "Subscriber should be complete");
    }


    @Test
    public void testFlux() {
        AtomicInteger entryCounter = new AtomicInteger(0);
        Sinks.Empty<Object> waitForThis = Sinks.empty();
        AtomicInteger exitCounter = new AtomicInteger(0);
        Flux<Void> protectThisThing = increment(entryCounter)
                .then(waitForThis.asMono())
                .then(increment(exitCounter))
                .flux();
        ReactiveSemaphore semaphore = ReactiveSemaphore.create("testCase");
        Flux<Void> protectedBySemaphore = protectThisThing.transform(semaphore::apply);

        TestSubscriber<Void> subscriber1 = TestSubscriber.builder().initialRequest(0).build();
        TestSubscriber<Void> subscriber2 = TestSubscriber.builder().initialRequest(0).build();

        log.info("Starting subscriber 1");
        protectedBySemaphore.subscribe(subscriber1);
        assertEquals(1, entryCounter.get(), "First subscriber should have entered the protected resource");
        assertEquals(0, exitCounter.get(), "First subscriber should still be waiting");
        assertFalse(subscriber1.isTerminatedOrCancelled(), "First subscriber should still be waiting");

        log.info("Starting subscriber 2");
        protectedBySemaphore.subscribe(subscriber2);
        assertEquals(1, entryCounter.get(), "Second subscriber should be waiting for semaphore");
        assertEquals(0, exitCounter.get(), "First subscriber should still be waiting");
        assertFalse(subscriber1.isTerminatedOrCancelled(), "First subscriber should still be waiting");
        assertFalse(subscriber2.isTerminatedOrCancelled(), "Second subscriber should still be waiting");

        log.info("Allowing subscribers to finish");
        waitForThis.tryEmitEmpty();
        assertEquals(2, entryCounter.get(), "Both subscribers should be complete");
        assertEquals(2, exitCounter.get(), "Both subscribers should be complete");
        assertTrue(subscriber1.isTerminatedOrCancelled(), "First subscriber should be complete");
        assertTrue(subscriber2.isTerminatedOrCancelled(), "Second subscriber should be complete");
    }

    @Test
    public void testFluxReentrant() {
        AtomicInteger entryCounter = new AtomicInteger(0);
        Flux<Void> protectThisThing = increment(entryCounter).flux();
        ReactiveSemaphore semaphore = ReactiveSemaphore.create("testCase");

        Flux<Void> anotherThing = protectThisThing
                .transform(semaphore::apply)
                // Now do some other operations
                .doOnSubscribe(s -> log.debug("Yep, we've subscribed"))
                .switchIfEmpty(Mono.empty())
                .map(v -> v)
                .doOnNext(v -> log.error("This wont happen"))
                // And protect this overall operation with the semaphore (again)
                .transform(semaphore::apply)
                // In case our re-entrant behavior is broken, don't block for long
                .timeout(Duration.ofMillis(500));

        TestSubscriber<Void> subscriber = TestSubscriber.builder().initialRequest(0).build();

        log.info("Starting subscriber");
        anotherThing.subscribe(subscriber);
        assertEquals(1, entryCounter.get(), "Subscriber should have obtained the semaphore and entered");
        assertTrue(subscriber.isTerminatedComplete(), "Subscriber should be complete");
    }

    private Mono<Void> increment(AtomicInteger counter) {
        return Mono.defer(() -> {
            counter.incrementAndGet();
            return Mono.empty();
        });
    }
}
