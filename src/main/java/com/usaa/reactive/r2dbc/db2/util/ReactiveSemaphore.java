package com.usaa.reactive.r2dbc.db2.util;

import com.ibm.asyncutil.locks.FairAsyncSemaphore;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A reactor-compatible implementation of a semaphore, which can be used that only one subscriber can perform a
 * particular operation at a time. Any given Mono or Flux can be "protected" by a semaphore instance like so:
 * <pre>{@code
 * ReactiveSemaphore mySemaphoreInstance = ReactiveSemaphore.create("superCool");
 * Mono<Foo> someCoolMono = ...
 * Mono<Foo> onlyOneAtATime = someCoolMono.transform(mySemaphoreInstance::apply);
 * return onlyOneAtATime;
 * }</pre>
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ReactiveSemaphore {
    private static final AtomicLong SEMAPHORE_ID = new AtomicLong(0);
    private static final Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(120); // TODO: document this magic number - it's to prevent deadlocks

    /**
     * Creates an instance of ReactiveSemaphore, which can be used to protect access to a shared resource.
     * @param name a name for this semaphore instance. This is currently only used for debug-level logging to aid in
     *             troubleshooting.
     * @return an instance of ReactiveSemaphore
     */
    @Nonnull
    public static ReactiveSemaphore create(String name) {
        return new ReactiveSemaphore(name);
    }

    private final String name;
    private final long id = SEMAPHORE_ID.getAndIncrement();
    // A lock-free algorithm, neat!
    private final FairAsyncSemaphore semaphore = new FairAsyncSemaphore(1);


    public <T> Mono<T> apply(Mono<T> mono) {
        return Mono.defer(() -> {
            AtomicBoolean needToRelease = new AtomicBoolean(true);
            return acquireIfNotAlreadyHeld(needToRelease)
                    // To make this semaphore reentrant, make a note of the fact that we have already obtained a permit
                    .then(mono.contextWrite(Context.of(this, true)))
                    .doOnTerminate(() -> release(needToRelease))
                    .doOnCancel(() -> release(needToRelease));
        });
    }

    public <T> Publisher<T> apply(Flux<T> flux) {
        return Flux.defer(() -> {
            AtomicBoolean needToRelease = new AtomicBoolean(true);
            return acquireIfNotAlreadyHeld(needToRelease)
                    // To make this semaphore reentrant, make a note of the fact that we have already obtained a permit
                    .thenMany(flux.contextWrite(Context.of(this, true)))
                    .doOnTerminate(() -> release(needToRelease))
                    .doOnCancel(() -> release(needToRelease));
        });
    }

    private Mono<Void> acquireIfNotAlreadyHeld(AtomicBoolean needToRelease) {
        return Mono.deferContextual(c -> {
            @SuppressWarnings("ConstantConditions")
            boolean isSemaphoreAlreadyHeld = c.getOrDefault(this, false);
            if (isSemaphoreAlreadyHeld) {
                // If an upstream operator acquired the semaphore, we shouldn't release it - they should
                needToRelease.set(false);
                log.debug("Semaphore {}(id={}) already held", name, id);
                return Mono.empty();
            } else {
                // We're acquiring the semaphore, make a note of the fact that we need to release it
                needToRelease.set(true);
                return acquire();
            }
        });
    }

    private Mono<Void> acquire() {
        return Mono.defer(() -> Mono.fromFuture(semaphore.acquire(1).toCompletableFuture()))
                .doOnSubscribe(s -> log.debug("Attempting to acquire semaphore {}(id={})", name, id))
                .doOnSuccess(v -> log.debug("Acquired semaphore {}(id={})", name, id))
                .timeout(ACQUIRE_TIMEOUT);
    }


    private void release(AtomicBoolean needToRelease) {
        if (needToRelease.compareAndSet(true, false)) {
            log.debug("Releasing semaphore {}(id={})", name, id);
            semaphore.release(1);
        } else {
            log.debug("Already released semaphore {}(id={}) or held re-entrant, taking no action", name, id);
        }
    }
}
