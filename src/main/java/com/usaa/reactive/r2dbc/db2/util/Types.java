package com.usaa.reactive.r2dbc.db2.util;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.vertx.db2client.impl.drda.DB2RowId;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.RowId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Types {
    // https://github.com/eclipse-vertx/vertx-sql-client/blob/4.2.1/vertx-db2-client/src/main/java/io/vertx/db2client/impl/drda/ClientTypes.java#L260
    private static final Set<Class<?>> SUPPORTED_CLASSES = new HashSet<>(Arrays.asList(
            // ClientTypes.INTEGER, ClientTypes.BIGINT
            boolean.class,
            Boolean.class,
            double.class,
            Double.class,
            float.class,
            Float.class,
            int.class,
            Integer.class,
            long.class,
            Long.class,
            short.class,
            Short.class,
            BigDecimal.class,
            BigInteger.class,
            // ClientTypes.DOUBLE, ClientTypes.REAL, ClientTypes.DECIMAL
            double.class,
            Double.class,
            float.class,
            Float.class,
            BigDecimal.class,
            // ClientTypes.BIT, ClientTypes.BOOLEAN, ClientTypes.SMALLINT
            boolean.class,
            Boolean.class,
            char.class,
            Character.class,
            int.class,
            Integer.class,
            long.class,
            Long.class,
            short.class,
            Short.class,
            byte.class,
            Byte.class,
            BigDecimal.class,
            // ClientTypes.BINARY
            boolean.class,
            Boolean.class,
            byte.class,
            Byte.class,
            byte[].class,
            // ClientTypes.DATE
            java.time.LocalDate.class,
            java.sql.Date.class,
            String.class,
            // ClientTypes.TIME
            java.time.LocalTime.class,
            java.sql.Time.class,
            String.class,
            // ClientTypes.TIMESTAMP
            java.time.LocalDateTime.class,
            java.sql.Timestamp.class,
            String.class,
            // ClientTypes.CHAR
            char.class,
            Character.class,
            String.class,
            // ClientTypes.VARCHAR, ClientTypes.LONGVARCHAR, ClientTypes.CLOB
            String.class,
            char[].class,
            UUID.class,
            // ClientTypes.VARBINARY, ClientTypes.LONGVARBINARY, ClientTypes.BLOB
            byte[].class,
            ByteBuf.class,
            // ClientTypes.ROWID
            RowId.class,
            DB2RowId.class,

            // R2DBC types
            Clob.class,
            Blob.class
    ));

    public static boolean isSupportedType(Class<?> clazz) {
        return SUPPORTED_CLASSES.stream()
            .anyMatch(supportedClass -> supportedClass.isAssignableFrom(clazz));
    }
}
