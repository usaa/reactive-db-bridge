package com.usaa.reactive.r2dbc.db2;

import com.usaa.reactive.r2dbc.db2.util.HandlerSupport;
import com.usaa.reactive.r2dbc.db2.util.ReactiveSemaphore;
import io.r2dbc.spi.*;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static com.usaa.reactive.r2dbc.db2.DB2ConnectionFactoryProvider.checkNotNull;

@Slf4j
/* package private */ class DB2Connection implements Connection {
    private static final String  SAVEPOINT_NAME_REGEX = "[-a-zA-Z0-9_]+";
    private static final Pattern SAVEPOINT_NAME_PATTERN = Pattern.compile(SAVEPOINT_NAME_REGEX);
    private static final AtomicLong CONNECTION_ID = new AtomicLong(0);

    private final io.vertx.db2client.DB2Connection conn;
    private final long connectionId = CONNECTION_ID.incrementAndGet();
    private final ReactiveSemaphore onlyOneTransactionOperationAtATime = ReactiveSemaphore.create("transaction-lock-" + connectionId);

    private volatile boolean isClosed = false;
    private volatile boolean isAutoCommit = true;
    private volatile Transaction activeTransaction = null;
    /*
     * Why READ_COMMITTED as the default? Well...
     * https://www.ibm.com/docs/en/db2/11.5?topic=issues-isolation-levels
     *      "CS is the default isolation level"
     * https://www.ibm.com/docs/en/db2/11.5?topic=statements-set-current-isolation
     *      "READ COMMITTED is recognized and upgraded to CS"
     */
    private volatile IsolationLevel currentIsolationLevel = IsolationLevel.READ_COMMITTED;


    public DB2Connection(io.vertx.db2client.DB2Connection conn) {
        this.conn = conn;
        log.debug("Connection {} - Connection opened", connectionId);
        conn.closeHandler(v -> {
            log.debug("Connection {} - Marking connection closed", connectionId);
            this.isClosed = true;
        });
    }

    @Override
    public Statement createStatement(String sql) {
        checkNotNull(sql, "sql");
        return new DB2Statement(connectionId, conn, sql, this::autoCommitDecorator);
    }

    @Override
    public Batch createBatch() {
        return new DB2Batch(connectionId, conn, this::autoCommitDecorator);
    }

    @Override
    public Mono<Void> close() {
        // First, roll back any active transaction
        return getActiveTransaction()
                .flatMap(tx -> HandlerSupport.<Void>asMono(tx::rollback)
                        .doOnSubscribe(s -> log.debug("Connection {} - Connection close requested, but transaction still active - rolling back transaction", connectionId))
                )
                // Then close the connection
                .then(HandlerSupport.<Void>asMono(conn::close))
                .doOnSubscribe(s -> log.debug("Connection {} - Closing connection", connectionId))
                .doOnSuccess(v -> {
                    log.debug("Connection {} - Connection close complete", connectionId);
                    this.isClosed = true;
                })
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.close() called", connectionId));
    }


    // Auto commit
    @Override
    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    @Override
    public Mono<Void> setAutoCommit(final boolean desired) {
        return Mono.defer(() -> Mono.just(this.isAutoCommit))
                // Check whether the new value differs from the current value
                .filter(current -> current != desired)
                // ...yes it does
                .doOnNext(current -> log.debug("Connection {} - Setting autoCommit to {} (currently is {})", connectionId, desired, current))
                // Update the flag (this is thread safe thanks to the semaphore)
                .doOnNext(current -> this.isAutoCommit = desired)
                // If there is an active transaction, commit it
                .flatMap(current -> getActiveTransaction().flatMap(tx -> commitTransaction()))
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.setAutoCommit(...) called", connectionId));
    }

    private <T> Flux<T> autoCommitDecorator(Flux<T> flux) {
        return Flux.defer(() -> {
            if (isAutoCommit) {
                // On subscription, start a transaction
                return HandlerSupport.<Transaction>asMono(conn::begin)
                        // Then do the things and commit
                        .flatMapMany(tx -> flux
                                // After the statement/batch is done, commit the transaction
                                .concatWith(HandlerSupport.<Void>asMono(tx::commit)
                                        .doOnSuccess(v -> log.debug("Connection {} - Transaction for autoCommit completed successfully", connectionId))
                                        .transform(this::castEmpty)
                                )
                                // If something goes wrong, rollback and then re-propagate the error
                                .onErrorResume(t -> HandlerSupport.<Void>asMono(tx::rollback)
                                        .doOnSuccess(v -> log.debug("Connection {} - Transaction for autoCommit rolled back after error", connectionId))
                                        .then(Mono.error(t)))
                                // On cancel, let's still rollback - but we can't make the caller "wait" for the outcome
                                .doOnCancel(() -> {
                                    @SuppressWarnings("unused")
                                    Disposable rollbackAsyncAndResultIsIgnored =
                                            HandlerSupport.<Void>asMono(tx::rollback)
                                                    .doOnSuccess(v -> log.debug("Connection {} - Transaction for autoCommit rolled back after cancellation", connectionId))
                                                    .subscribe();
                                }))
                        // Only one auto-commit statement/batch can be in-flight at any given time, and you can't begin
                        // transaction until it is done
                        .transform(onlyOneTransactionOperationAtATime::apply)
                        .doOnSubscribe(s -> log.debug("Connection {} - Connection is in autoCommit mode, starting a one-off transaction", connectionId));
            } else {
                // If auto-commit is disabled, we assume the client already started their transaction. It is fine to
                // have multiple queries executing concurrently within the scope of that transaction
                return flux;
            }
        });
    }

    // Put this cast in a helper method so we can suppress the type warnings
    @SuppressWarnings({"unchecked","rawtypes"})
    private <T> Mono<T> castEmpty(Mono<Void> empty) {
        // No value will be returned by the Mono, so it is safe to cast the value type
        return (Mono) empty;
    }

    // Transactions
    private Mono<Transaction> getActiveTransaction() {
        return Mono.defer(() -> Mono.justOrEmpty(this.activeTransaction));
    }

    @Override
    public Mono<Void> beginTransaction() {
        return getActiveTransaction()
                .doOnSubscribe(s -> this.isAutoCommit = false)
                .doOnNext(tx -> log.debug("Connection {} - Transaction already active, not starting another", connectionId))
                .switchIfEmpty(HandlerSupport.<Transaction>asMono(conn::begin)
                        .doOnSubscribe(s -> log.debug("Connection {} - Beginning transaction", connectionId))
                        .doOnSuccess(tx -> this.activeTransaction = tx)
                        .doOnSuccess(v -> log.debug("Connection {} - Transaction begin succeeded", connectionId))
                )
                .then()
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.beginTransaction() called", connectionId));
    }

    @Override
    public Mono<Void> commitTransaction() {
        return getActiveTransaction()
                .doOnSuccess(tx -> {
                    if (tx == null) {
                        log.debug("Connection {} - No transaction is active, ignoring commit", connectionId);
                    } else {
                        log.debug("Connection {} - Committing transaction", connectionId);
                    }
                })
                .flatMap(tx -> HandlerSupport.<Void>asMono(tx::commit))
                .doOnSuccess(v -> log.debug("Connection {} - Transaction commit succeeded", connectionId))
                .doOnTerminate(() -> this.activeTransaction = null)
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.commitTransaction() called", connectionId));
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
        return getActiveTransaction()
                .doOnSuccess(tx -> {
                    if (tx == null) {
                        log.debug("Connection {} - No transaction is active, ignoring rollback", connectionId);
                    } else {
                        log.debug("Connection {} - Rolling back transaction", connectionId);
                    }
                })
                .flatMap(tx -> HandlerSupport.<Void>asMono(tx::rollback))
                .doOnSuccess(v -> log.debug("Connection {} - Transaction rollback succeeded", connectionId))
                .doOnTerminate(() -> this.activeTransaction = null)
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.rollbackTransaction() called", connectionId));
    }

    // Isolation level
    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return currentIsolationLevel;
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(final IsolationLevel isolationLevel) {
        checkNotNull(isolationLevel, "isolationLevel");
        // https://www.ibm.com/docs/en/db2/11.5?topic=statements-set-current-isolation
        return query("SET CURRENT ISOLATION LEVEL = ?", isolationLevel.asSql())
                .doOnSubscribe(s -> log.debug("Connection {} - Setting transaction isolation level to {} (currently is {})", connectionId, isolationLevel, currentIsolationLevel))
                // Once the query completes, update our local isolation level
                .doOnSuccess(s -> currentIsolationLevel = isolationLevel)
                // The SQL may have failed, or it may have succeeded but we timed out waiting for the response, so we don't know the current state
                .doOnError(t -> {
                    log.warn("Connection {} - Unable to set transaction isolation level to {}", connectionId, isolationLevel, t);
                    currentIsolationLevel = IsolationLevel.valueOf("UNKNOWN");
                })
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.setTransactionIsolationLevel(...) called", connectionId));
    }

    // Savepoints
    @Override
    public Mono<Void> createSavepoint(String name) {
        checkNotNull(name, "name");
        validateSavepointName(name);
        return getActiveTransaction()
                // The TCK and Spec expects us to start a transaction if one is not already active (even though it isn't mentioned in the Javadoc)
                // https://r2dbc.io/spec/0.8.6.RELEASE/spec/html/#transactions.savepoints
                .switchIfEmpty(beginTransaction().then(getActiveTransaction()))
                // https://www.ibm.com/docs/en/db2/10.5?topic=statements-savepoint
                .then(query("SAVEPOINT " + name + " ON ROLLBACK RETAIN CURSORS"))
                .doOnSubscribe(s -> log.debug("Connection {} - Creating savepoint with name {}", connectionId, name))
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.createSavepoint(...) called", connectionId));
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        checkNotNull(name, "name");
        validateSavepointName(name);
        // https://www.ibm.com/docs/en/db2/10.5?topic=statements-savepoint
        return query("RELEASE SAVEPOINT " + name)
                .doOnSubscribe(s -> log.debug("Connection {} - Releasing savepoint with name {}", connectionId, name))
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.releaseSavepoint(...) called", connectionId));
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        checkNotNull(name, "name");
        validateSavepointName(name);
        // https://www.ibm.com/docs/en/db2/10.5?topic=statements-rollback
        return query("ROLLBACK TO SAVEPOINT " + name)
                .doOnSubscribe(s -> log.debug("Connection {} - Rolling back to savepoint with name {}", connectionId, name))
                // Once subscribed, other transaction-related operations have to wait until this completes
                .transform(onlyOneTransactionOperationAtATime::apply)
                .doOnSubscribe(s -> log.debug("Connection {} - Connection.rollbackTransactionToSavepoint(...) called", connectionId));
    }

    private void validateSavepointName(String name) {
        if (!SAVEPOINT_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Savepoint name is invalid; must match pattern " + SAVEPOINT_NAME_REGEX);
        }
    }

    private Mono<Void> query(String sql, String... params) {
        return HandlerSupport.<RowSet<Row>>asMono(h -> conn.preparedQuery(sql).execute(Tuple.from(params), h))
                .doOnSubscribe(s -> log.debug("Connection {} - Connection executing: {}", connectionId, sql))
                .doOnSuccess(rs -> log.debug("Connection {} - Connection done executing: {}", connectionId, sql))
                .then();
    }

    // Other
    @Override
    public ConnectionMetadata getMetadata() {
        return new ConnectionMetadata() {
            @Override
            public String getDatabaseProductName() {
                return conn.databaseMetadata().productName();
            }
            @Override
            public String getDatabaseVersion() {
                return conn.databaseMetadata().fullVersion();
            }
        };
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        checkNotNull(depth, "depth");
        return Mono.defer(() -> {
            log.debug("Connection {} - Validating connection with depth {}", connectionId, depth);
            switch (depth) {
                case LOCAL:
                    return Mono.just(!isClosed);
                case REMOTE:
                    return HandlerSupport.<Void>asMono(conn::ping)
                            // Convert the empty Mono<Void> to a Mono<Boolean>, emitting true
                            .cast(Boolean.class).defaultIfEmpty(true)
                            // If something goes wrong, emit false
                            .onErrorReturn(false);
                default:
                    throw new IllegalArgumentException("Depth " + depth.toString() + " not recognized");
            }
        });
    }
}