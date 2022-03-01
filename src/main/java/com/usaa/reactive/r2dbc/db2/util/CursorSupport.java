package com.usaa.reactive.r2dbc.db2.util;

import io.vertx.core.Handler;
import io.vertx.sqlclient.Cursor;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

@Slf4j
public class CursorSupport {
    /**
     * Creates a Flux that will iterate over the provided cursor on subscription. An error will be thrown if the Flux is
     * subscribed more than once.
     * @param cursor A Vert.X cursor, obtained via {@link PreparedStatement#cursor()}
     * @param fetchSize The fetch size to pass to {@link Cursor#read(int, Handler)}
     * @return A Flux over the RowSet's returned by the cursor. The cursor will be closed when this flux completes or is
     * cancelled.
     */
    public static Flux<RowSet<Row>> iterateCursor(Cursor cursor, int fetchSize) {
        return new CursorIterator(cursor, fetchSize).asFlux();
    }

    @RequiredArgsConstructor
    private static class CursorIterator {
        private final Cursor cursor;
        private final int fetchSize;
        private final Sinks.Many<Mono<RowSet<Row>>> sink = Sinks.many().unicast().onBackpressureBuffer();

        // This flag serves as a lock to make sure we don't try to perform a read while a previous read is still pending
        private final AtomicBoolean isActive    = new AtomicBoolean(false);
        // If the downstream cancels, we need to stop iterating the cursor and close it
        private volatile boolean    isCancelled = false;
        // In order to honor backpressure, we need to keep track of downstream demand
        @SuppressWarnings("FieldMayBeFinal") // Field is updated reflectively by AtomicLongFieldUpdater
        private volatile long requested = 0;
        private static final AtomicLongFieldUpdater<CursorIterator> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(CursorIterator.class, "requested");

        public Flux<RowSet<Row>> asFlux() {
            Flux<Mono<RowSet<Row>>> fluxOfAsyncCunksFromCursor = sink.asFlux()
                    // The first read() on the cursor won't be triggered until downstream signals demand
                    .doOnRequest(this::onRequest)
                    // Stop reading after downstream cancels their subscription
                    .doOnCancel(() -> this.isCancelled = true);
            return Flux.concat(fluxOfAsyncCunksFromCursor, 1);
        }

        private void onRequest(long additionalDemand) {
            Operators.addCap(REQUESTED, this, additionalDemand);
            // Now that we have some demand, kick off the cursor (as long as it's not already running)
            maybeIterateCursor();
        }

        private void maybeIterateCursor() {
            // If there is currently demand, try to mark ourselves active, then perform the read
            if (requested > 0 && isActive.compareAndSet(false, true)) {
                performRead();
            }
        }

        private void performRead() {
            // Fetch the next chunk of rows from the cursor
            Mono<RowSet<Row>> nextChunkIsPending = HandlerSupport.<RowSet<Row>>asMono(h -> cursor.read(fetchSize, h))
                    .doOnSubscribe(r -> log.debug("Fetching next chunk from cursor"))
                    // Once the cursor returns some rows, we'll check if we should get more
                    .doOnNext(this::handleCursorResult);
            // Send this chunk up to the Flux.concat(...)
            sink.tryEmitNext(nextChunkIsPending);
            // Decrement the request value. This enables us to honor backpressure: we won't keep pulling from the cursor
            // if there's no demand downstream
            Operators.produced(REQUESTED, this, 1);
        }

        private void handleCursorResult(RowSet<Row> rs) {
            log.debug("Cursor returned {} rows", rs.rowCount());
            // Now that the cursor has returned us some results, we can clear the "isActive" flag
            isActive.set(false);
            // Check if the cursor has more data for us to pull
            if (cursor.hasMore() && !cursor.isClosed()) {
                // Make sure the downstream still wants us to keep sending data
                if (isCancelled) {
                    log.debug("Cursor has more data but Subscription was cancelled, closing cursor");
                    cursor.close();
                } else {
                    // Pull the next chunk from the cursor (if there is downstream demand)
                    maybeIterateCursor();
                }
            } else {
                // We aren't expected to call close() on a cursor that doesn't have more
                log.debug("Cursor has no more data");
                // Signal completion up to the Flux.concat(...)
                sink.tryEmitComplete();
            }
        }
    }
}
