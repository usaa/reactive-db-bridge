package com.usaa.reactive.r2dbc.db2;

import com.google.auto.service.AutoService;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.vertx.core.Vertx;

import javax.annotation.Nullable;

@AutoService(ConnectionFactoryProvider.class)
public class DB2ConnectionFactoryProvider implements ConnectionFactoryProvider {
    public static final String DB2_DRIVER = "vertx-db2";

    @Override
    public String getDriver() {
        return DB2_DRIVER;
    }

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions options) {
        checkNotNull(options, "options");
        return new DB2ConnectionFactory(Vertx.vertx(), options);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions options) {
        checkNotNull(options, "options");
        return DB2_DRIVER.equals(options.getRequiredValue(ConnectionFactoryOptions.DRIVER));
    }

    public static void checkNotNull(@Nullable Object o, String name) {
        if (o == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
