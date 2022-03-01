package com.usaa.reactive.r2dbc.spring;

import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindTarget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DB2R2dbcDialectTest {
    private static final String VALUE_STR = "Hello";
    private static final int    VALUE_INT = 42;

    @Test
    void testBindMarkers() {
        BindMarkers bindMarkers = new DB2R2dbcDialect().getBindMarkersFactory().create();
        assertNotNull(bindMarkers, "Bind markers factory returned null!");

        BindMarker bindMarker = bindMarkers.next();
        BindTarget target = mock(BindTarget.class);
        assertEquals("?", bindMarker.getPlaceholder(), "UDB Dialect uses ? for bind placeholders");
        bindMarker.bind(target, VALUE_STR);
        verify(target).bind(0, VALUE_STR);

        bindMarker = bindMarkers.next();
        target = mock(BindTarget.class);
        assertEquals("?", bindMarker.getPlaceholder(), "UDB Dialect uses ? for bind placeholders");
        bindMarker.bind(target, VALUE_INT);
        verify(target).bind(1, VALUE_INT);

        bindMarker = bindMarkers.next();
        target = mock(BindTarget.class);
        assertEquals("?", bindMarker.getPlaceholder(), "UDB Dialect uses ? for bind placeholders");
        bindMarker.bindNull(target, Boolean.class);
        verify(target).bindNull(2, Boolean.class);
    }
}
