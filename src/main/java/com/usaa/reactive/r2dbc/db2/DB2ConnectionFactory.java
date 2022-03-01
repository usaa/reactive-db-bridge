package com.usaa.reactive.r2dbc.db2;

import com.usaa.reactive.r2dbc.db2.util.HandlerSupport;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.vertx.core.Vertx;
import io.vertx.db2client.DB2ConnectOptions;
import org.reactivestreams.Publisher;

import java.util.Optional;

public class DB2ConnectionFactory implements ConnectionFactory {
    private final Vertx vertx;
    private final DB2ConnectOptions connectOptions;

    public DB2ConnectionFactory(Vertx vertx, ConnectionFactoryOptions options) {
        this.vertx = vertx;
        this.connectOptions = new DB2ConnectOptions();

        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.HOST)).ifPresent(connectOptions::setHost);
        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.PORT)).ifPresent(connectOptions::setPort);
        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.DATABASE)).ifPresent(connectOptions::setDatabase);
        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.USER)).ifPresent(connectOptions::setUser);
        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.PASSWORD)).map(CharSequence::toString).ifPresent(connectOptions::setPassword);
        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.CONNECT_TIMEOUT)).map(d -> (int) d.toMillis()).ifPresent(connectOptions::setConnectTimeout);
        Optional.ofNullable(options.getValue(ConnectionFactoryOptions.SSL)).ifPresent(connectOptions::setSsl);
    }

    @Override
    public Publisher<? extends Connection> create() {
        return HandlerSupport.<io.vertx.db2client.DB2Connection>
                asMono(h -> io.vertx.db2client.DB2Connection.connect(vertx, connectOptions, h))
                        .map(DB2Connection::new);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return () -> "DB2";
    }
}
