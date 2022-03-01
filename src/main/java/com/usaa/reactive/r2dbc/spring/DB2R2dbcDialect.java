package com.usaa.reactive.r2dbc.spring;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.dialect.Db2Dialect;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.r2dbc.core.binding.BindMarkersFactoryResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class DB2R2dbcDialect extends Db2Dialect implements R2dbcDialect {
    private static final BindMarkersFactory BIND_MARKERS_FACTORY = BindMarkersFactory.anonymous("?");
    private static final DB2R2dbcDialect INSTANCE = new DB2R2dbcDialect();

    public static class Provider implements DialectResolver.R2dbcDialectProvider,
            BindMarkersFactoryResolver.BindMarkerFactoryProvider {
        @Override
        public Optional<R2dbcDialect> getDialect(ConnectionFactory connectionFactory) {
            return Optional.of(connectionFactory.getMetadata().getName())
                    .filter("DB2"::equals)
                    .map(name -> INSTANCE);
        }

        @Override
        public BindMarkersFactory getBindMarkers(ConnectionFactory connectionFactory) {
            return Optional.of(connectionFactory.getMetadata().getName())
                    .filter("DB2"::equals)
                    .map(name -> BIND_MARKERS_FACTORY)
                    .orElse(null);
        }
    }

    @Override
    public BindMarkersFactory getBindMarkersFactory() {
        return BIND_MARKERS_FACTORY;
    }

    @Override
    public Collection<Object> getConverters() {
        List<Object> converters = new ArrayList<>(super.getConverters());
        converters.add(new BooleanConverter());
        return converters;
    }
}
