package com.usaa.reactive.r2dbc.db2;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.vertx.sqlclient.Row;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static com.usaa.reactive.r2dbc.db2.DB2ConnectionFactoryProvider.checkNotNull;

@RequiredArgsConstructor
/* package private */ class DB2Row implements io.r2dbc.spi.Row {
    private final Row row;
    private final DB2RowMetadata metadata;

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int index, Class<T> type) {
        checkNotNull(type, "type");
        if (Object.class.equals(type)) {
            Class<?> defaultType = metadata.getColumnMetadata(index).getJavaType();
            // Did we find a default?
            if (defaultType == null) {
                return (T) row.getValue(index);
            } else {
                return (T) getCoerced(index, defaultType);
            }
        } else {
            return getCoerced(index, type);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getCoerced(int index, Class<T> type) {
        if (ByteBuffer.class.equals(type)) {
            return (T) ByteBuffer.wrap(row.getBuffer(index).getBytes());
        } else if (Blob.class.equals(type)) {
            return (T) Blob.from(Mono.just(ByteBuffer.wrap(row.getBuffer(index).getBytes())));
        } else if (Clob.class.equals(type)) {
            return (T) Clob.from(Mono.just(row.getString(index)));
        } else {
            return row.get(type, index);
        }
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        checkNotNull(name, "name");
        checkNotNull(type, "type");
        return get(metadata.getColumnIndex(name), type);
    }


}
