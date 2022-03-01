package com.usaa.reactive.r2dbc.db2;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static com.usaa.reactive.r2dbc.db2.DB2ConnectionFactoryProvider.checkNotNull;

@RequiredArgsConstructor
/* package private */ class DB2Batch implements Batch {
    private final long connectionId;
    private final io.vertx.db2client.DB2Connection conn;
    private final List<String> statements = new ArrayList<>();
    private final UnaryOperator<Flux<? extends Result>> executionDecorator;

    @Override
    public Batch add(String sql) {
        checkNotNull(sql, "sql");
        statements.add(sql);
        return this;
    }

    @Override
    public Flux<? extends Result> execute() {
        return Flux.fromIterable(statements)
                .map(sql -> new DB2Statement(connectionId, conn, sql, UnaryOperator.identity()))
                .flatMap(DB2Statement::execute) // TODO: test concurrency/pipelining support
                // Wrap the whole batch execution as needed (e.g. transaction for auto-commit)
                .transformDeferred(executionDecorator::apply);
    }
}
