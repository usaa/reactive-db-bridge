package com.usaa.reactive.r2dbc.db2;

import com.usaa.reactive.r2dbc.db2.util.HandlerSupport;
import com.usaa.reactive.r2dbc.db2.util.ReactiveSemaphore;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.RowMetadata;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.usaa.reactive.r2dbc.db2.util.CursorSupport.iterateCursor;

@Slf4j
@RequiredArgsConstructor
/* package private */ class DB2Result {
    private final long connectionId;
    private final PreparedStatement preparedStatement;
    private final List<Tuple> batch;
    private final int fetchSize;
    // The SQL is only used for logging
    private final String sql;

    private final ReactiveSemaphore oneResultAtATime = ReactiveSemaphore.create("statement-result");
    /*
     * The semaphore above effectively synchronizes access to the below so we don't need to use AtomicReference(Updater)
     * to get atomic Compare And Set behavior. We can simply read the value, and then some time later update it - both
     * take place within the bounds of the semaphore so there's no chance of two threads both reading null and deciding
     * to proceed.
     */
    private volatile List<Integer> batchUpdateRowCounts = null;
    private volatile boolean isCursorObtainedForAnyResult = false;


    /* package private */ Flux<? extends Result> stream() {
        // Create a ResultView object for each tuple in the batch
        List<ResultView> results = IntStream.range(0, batch.size())
                .mapToObj(ResultView::new)
                .collect(Collectors.toList());
        // Return a Flux over the results...
        return Flux.fromIterable(results)
                // ... but that Flux doesn't complete until each result has been subscribed to and the result Publisher completed
                .concatWith(Flux.fromIterable(results).flatMap(ResultView::getCompletion));
    }

    /*
     * The RR2DBC spec is a little annoying here: a given result will be either used to get a row count, *or* to iterate
     * over each row. But the caller isn't obligated to tell us in advance which case it will be. It's not until we give
     * them a Result and they call one method (getRowsUpdated) or the other (map) that we know.
     * Moreover, the spec (https://r2dbc.io/spec/0.8.6.RELEASE/spec/html/#statements.batching) says:
     *      A batch run emits one or many Result objects, depending on how the implementation executes the batch.
     *
     * But the TCK runs a statement batch with 10 sets of bind variables, then asserts that 10 Result objects are
     * emitted from Statement.execute:
     *      https://github.com/r2dbc/r2dbc-spi/blob/v0.8.6.RELEASE/r2dbc-spi-test/src/main/java/io/r2dbc/spi/test/TestKit.java#L581
     *
     * If the TCK relaxes this test, to allow for just returning one Result we could potentially have a simpler
     * implementation here (not needing to have one class for the overall result, and many ResultView's to present
     * individual results to the caller). However, the code is already written so we might as well keep it this way.
     * Maybe a client could benefit from knowing which .map(...) rows were related to the first set of bind variables,
     * which are related to the second, etc.     *shrug*
     */
    @RequiredArgsConstructor
    private class ResultView implements Result {
        private final int resultNumber;

        private final Sinks.Empty<ResultView> completionSink = Sinks.empty();
        @Getter
        private final Mono<ResultView> completion = completionSink.asMono();

        private volatile boolean isCursorObtainedForThisResult = false;

        @Override
        public Mono<Integer> getRowsUpdated() {
            // This null check (via justOrEmpty deferred until subscribe) is thread safe because (1) "batchUpdateCounts"
            // is volatile, and (2) this publisher is protected by the "oneResultAtATime" semaphore so we'll never have
            // two subscribers do the null check concurrently
            return Mono.defer(() -> Mono.justOrEmpty(batchUpdateRowCounts))
                    // We don't want to execute all the statements as a batch of updates if any result has already been executed via map(...)
                    .doOnSubscribe(s -> {
                        if (isCursorObtainedForAnyResult) {
                            throw new IllegalStateException("Once map(...) has been called on any Result from a Statement, getRowsUpdated() may not be called on subsequent Results from the same Statement.");
                        }
                    })
                    // The first result on which getRowsUpdated() gets called will execute the batch and save off the row counts
                    .switchIfEmpty(HandlerSupport.<RowSet<Row>>asMono(h -> preparedStatement.query().executeBatch(batch, h))
                            .doOnSubscribe(s -> log.debug("Connection {} - Executing statement with {} sets of params: {}", connectionId, batch.size(), sql))
                            .map(this::getUpdateCounts)
                            // Save the update counts, to be re-used by the next Result's invocation of getRowsUpdated()
                            .doOnNext(counts -> DB2Result.this.batchUpdateRowCounts = counts)
                    )
                    // Get the update count for this result
                    .map(list -> list.get(resultNumber))
                    // Once we're done (success/error/cancel), trigger the completion sink
                    .doFinally(s -> completionSink.tryEmitEmpty())
                    // Only one result can be subscribed to at a time
                    .transform(oneResultAtATime::apply);
        }

        /*
         * RowSet chooses to implement Iterable for rows, but only provides half an iterator (i.e. a next() method but
         * not a hasNext()) to navigate the multiple RowSets that are returned from an executeBatch(). Thus, we have
         * this helper to iterate over each RowSet, grabbing the rowCount() from each one.
         */
        private List<Integer> getUpdateCounts(RowSet<Row> rowSet) {
            RowSet<Row> current = rowSet;
            List<Integer> rowCounts = new ArrayList<>(batch.size());
            do {
                rowCounts.add(current.rowCount());
            } while ((current = current.next()) != null);
            return rowCounts;
        }

        @Override
        public <T> Flux<T> map(BiFunction<io.r2dbc.spi.Row, RowMetadata, ? extends T> mappingFunction) {
            // Nothing special about "this", could be anything. The value is ignored by the first flatMap anyways
            return Flux.just(this)
                    .doOnSubscribe(s -> {
                        // Check that we haven't already run all the queries as a batch
                        if (batchUpdateRowCounts != null) {
                            throw new IllegalStateException("Once getRowsUpdated() has been called on any Result from a Statement, map(...) may not be called on subsequent Results from the same Statement.");
                        }
                        // Check that we haven't already gotten a cursor for this item in the batch
                        if (isCursorObtainedForThisResult) {
                            throw new IllegalStateException("map(...) has already been called on this Result and cannot be called again.");
                        }
                        // Make a note of the fact that we have obtained a cursor
                        isCursorObtainedForThisResult = true;
                        isCursorObtainedForAnyResult = true;
                        log.debug("Connection {} - Getting cursor {} for statement: {}", connectionId, resultNumber, sql);
                    })
                    // Get a cursor and turn it into a Flux of RowSet's (there will be one per Cursor.next() call)
                    .flatMap(ignored -> iterateCursor(preparedStatement.cursor(batch.get(resultNumber)), fetchSize))
                    // Apply the mapping function for each Row returned in each RowSet
                    .flatMap(rowSet -> {
                        // We only want to create one metadata object for the whole chunk of rows to share
                        DB2RowMetadata metadata = new DB2RowMetadata(rowSet.columnDescriptors());
                        return Flux.fromIterable(rowSet)
                                .map(row -> (T) mappingFunction.apply(new DB2Row(row, metadata), metadata));
                    // The mapping function probably shouldn't depend on being called sequentially and should be safe to
                    // call concurrently (i.e. it should be a pure function) - but just in case it isn't, set
                    // concurrency on the flatMap to 1. So if the cursor gives us a second RowSet while we're still
                    // iterating the first one, we won't start doing row mapping on the second until the first has been
                    // exhausted. Also this gives us better backpressure, since the default concurrency is 256. If each
                    // RowSet is, for example, 100 rows (per the value of "fetchSize") then buffering 256 RowSets of 100
                    // rows each (25.6K rows total) is a whole lot to hold in memory
                    }, 1)
                    // Once we're done (success/error/cancel), trigger the completion sink
                    .doFinally(s -> completionSink.tryEmitEmpty())
                    // Only one result can be subscribed to at a time
                    .transform(oneResultAtATime::apply);
        }
    }
}
