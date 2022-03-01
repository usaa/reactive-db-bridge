package com.usaa.reactive.r2dbc.db2;

import com.usaa.reactive.r2dbc.db2.util.HandlerSupport;
import com.usaa.reactive.r2dbc.db2.util.Types;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.vertx.db2client.DB2Connection;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static com.usaa.reactive.r2dbc.db2.DB2ConnectionFactoryProvider.checkNotNull;
import static com.usaa.reactive.r2dbc.db2.util.LobSupport.collectLOBs;

@Slf4j
/* package private */ class DB2Statement implements Statement {
    private static final String  COLUMN_NAME_REGEX = "[-a-zA-Z0-9_]+";
    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile(COLUMN_NAME_REGEX);

    private final long connectionId;
    private final DB2Connection conn;
    private final String originalSql;
    private final int queryParamCount;
    private final UnaryOperator<Flux<? extends Result>> executionDecorator;
    private final List<List<Object>> batch = new ArrayList<>();
    private final AtomicBoolean hasExecuteBeenSubscribed = new AtomicBoolean(false);

    private static final int TRUE = 1, FALSE = 0; // Ugh! Where's AtomicBooleanFieldUpdater when you need one?
    private volatile int hasExecuteBeenCalled = FALSE;
    private static final AtomicIntegerFieldUpdater<DB2Statement> HAS_EXECUTE_BEEN_CALLED =
        AtomicIntegerFieldUpdater.newUpdater(DB2Statement.class, "hasExecuteBeenCalled");

    private volatile BindParameters currentParams = new BindParameters();
    private volatile String finalSql;
    // Zero is an acceptable default - the vert.x driver will take that to mean "default", matching the R2DBC expected behavior
    // https://github.com/eclipse-vertx/vertx-sql-client/blob/4.2.1/vertx-db2-client/src/main/java/io/vertx/db2client/impl/drda/DRDAQueryRequest.java#L1692
    private volatile int fetchSize = 0;

    public DB2Statement(long connectionId, DB2Connection conn, String sql, UnaryOperator<Flux<? extends Result>> executionDecorator) {
        this.connectionId = connectionId;
        this.conn = conn;
        this.originalSql = sql;
        this.finalSql = sql;
        this.executionDecorator = executionDecorator;
        // Count the number of ? characters in the SQL
        // TODO: this will be a problem if there is a string literal containing a ? char
        this.queryParamCount = (int) sql.chars().filter(ch -> ch == '?').count();
    }

    @Override
    public Statement add() {
        checkThatExecuteHasNotBeenCalled();
        if (currentParams.size() != queryParamCount) {
            throw new IllegalStateException("Expected " + queryParamCount + " bind values, but got " + currentParams);
        }
        batch.add(currentParams.toList());
        currentParams = new BindParameters();
        return this;
    }

    @Override
    public Statement bind(int index, Object value) {
        checkNotNull(value, "value");
        checkIndex(index);
        checkThatExecuteHasNotBeenCalled();
        currentParams.add(index, value);
        return this;
    }
    @Override
    public Statement bindNull(int index, Class<?> type) {
        checkIndex(index);
        checkThatExecuteHasNotBeenCalled();
        currentParams.add(index, null);
        return this;
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= queryParamCount) {
            throw new IndexOutOfBoundsException("Parameter index must be less than " + queryParamCount + " and greater than or equal to zero");
        }
    }

    @Override
    public Statement bind(String name, Object value) {
        checkNotNull(name, "name");
        return bind(parseBindParamAsInt(name), value);
    }
    @Override
    public Statement bindNull(String name, Class<?> type) {
        checkNotNull(name, "name");
        return bindNull(parseBindParamAsInt(name), type);
    }

    private int parseBindParamAsInt(String name) {
        try {
            return Integer.parseInt(name);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Name-based bind parameters are not supported, supply bind parameters by index instead", e);
        }
    }

    private void checkThatExecuteHasNotBeenCalled() {
        if (hasExecuteBeenCalled == TRUE) {
            throw new IllegalStateException("This statement has already been executed and cannot be reused");
        }
    }

    @Override
    public Flux<? extends Result> execute() {
        // Put the last set of params into the list of batch params
        add();
        // We don't allow for execute to be called more than once
        if (!HAS_EXECUTE_BEEN_CALLED.compareAndSet(this, FALSE, TRUE)) {
            throw new IllegalStateException("This statement has already been executed and cannot be reused");
        }
        // Create a prepared statement...
        return HandlerSupport.<PreparedStatement>asMono(h -> conn.prepare(finalSql, h))
                // ...meanwhile, round up any LOB data
                .zipWith(Mono.defer(() -> collectLOBs(batch).map(Tuple::from).collectList()))
                // And create the result once the PreparedStatement and bind values are both available
                .flatMapMany(pair -> new DB2Result(connectionId, pair.getT1(), pair.getT2(), fetchSize, originalSql).stream())
                // In case the caller doesn't log errors, turning on debug logging will catch it here
                .doOnError(t -> log.debug("Connection {} - Statement failed: {}", connectionId, originalSql, t))
                // Make sure the caller doesn't subscribe to this Flux multiple times
                .doOnSubscribe(s -> {
                    if (hasExecuteBeenSubscribed.compareAndSet(false, true)) {
                        // First subscription, all is well
                        log.debug("Connection {} - Executing statement: {}", connectionId, originalSql);
                    } else {
                        throw new IllegalStateException("This Statement's result has already been subscribed to, the publisher cannot be reused");
                    }
                })
                // Wrap the statement execution as needed (e.g. transaction for auto-commit)
                .transformDeferred(executionDecorator::apply);
    }

    @Override
    public Statement returnGeneratedValues(String... columns) {
        checkNotNull(columns, "columns");
        checkThatExecuteHasNotBeenCalled();
        // Validate column names (to protect against SQL injection, since we'll concatenate them into the SQL text
        Arrays.stream(columns)
                .filter(name -> !COLUMN_NAME_PATTERN.matcher(name).matches())
                .forEach(badName -> { throw new IllegalArgumentException("Return column name is invalid; must match pattern " + COLUMN_NAME_REGEX); });
        // Create an expression for the list of column names (or "*" as a default)
        String columnNames = String.join(", ", columns);
        if (columnNames.isEmpty()) {
            columnNames = "*";
        }
        // Update the SQL that we will be executing, per:
        // https://www.ibm.com/docs/en/db2/11.5?topic=statement-result-sets-from-sql-data-changes
        finalSql = String.format("SELECT %s FROM FINAL TABLE ( %s ) ORDER BY INPUT SEQUENCE", columnNames, originalSql);
        return this;
    }

    @Override
    public Statement fetchSize(int rows) {
        checkThatExecuteHasNotBeenCalled();
        fetchSize = rows;
        return this;
    }

    private static class BindParameters {
        private final SortedMap<Integer, Object> params = new TreeMap<>();

        public void add(int index, @Nullable Object value) {
            if (value != null && !Types.isSupportedType(value.getClass())) {
                throw new IllegalArgumentException("Unsupported type: " + value.getClass());
            }
            params.put(index, value);
        }

        public int size() {
            return params.size();
        }

        public List<Object> toList() {
            List<Object> values = new ArrayList<>(params.size());
            // Iterate over the sorted entries, ensuring there are no gaps in the parameter numbers
            for (Map.Entry<Integer, Object> entry : params.entrySet()) {
                if (entry.getKey() == values.size()) {
                    values.add(entry.getValue());
                } else {
                    throw new IllegalStateException("No value provided for parameter index " + values.size());
                }
            }
            return values;
        }
    }
}
