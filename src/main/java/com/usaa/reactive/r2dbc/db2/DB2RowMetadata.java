package com.usaa.reactive.r2dbc.db2;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import io.vertx.db2client.impl.drda.ClientTypes;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.util.*;

import static com.usaa.reactive.r2dbc.db2.DB2ConnectionFactoryProvider.checkNotNull;

/* package private */ class DB2RowMetadata implements RowMetadata {
    private final List<ColumnMetadata> columnMetadata;
    private final Map<String, Integer> columnIndexes;
    private final Collection<String> columnNames;

    public DB2RowMetadata(List<ColumnDescriptor> columnDescriptors) {
        List<ColumnMetadata> mutableColumnMetadata = new ArrayList<>(columnDescriptors.size());
        Map<String, Integer> mutableColumnIndexes = new HashMap<>();
        List<String>         mutableColumnNames = new ArrayList<String>() {
            @Override
            public boolean contains(Object o) {
                return stream().anyMatch(item -> item.equalsIgnoreCase(o.toString()));
            }
        };

        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
            int index = mutableColumnMetadata.size();
            mutableColumnMetadata.add(new DB2ColumnMetadata(columnDescriptor));
            mutableColumnIndexes.putIfAbsent(columnDescriptor.name().toLowerCase(), index);
            mutableColumnNames.add(columnDescriptor.name());
        }

        columnMetadata = Collections.unmodifiableList(mutableColumnMetadata);
        columnIndexes  = Collections.unmodifiableMap(mutableColumnIndexes);
        columnNames    = Collections.unmodifiableCollection(mutableColumnNames);
    }

    @Override
    public ColumnMetadata getColumnMetadata(int index) {
        // Note: we don't quite comply with spec here, we throw IndexOutOfBounds rather than ArrayIndexOutOfBounds
        return columnMetadata.get(index);
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
        checkNotNull(name, "name");
        return columnMetadata.get(getColumnIndex(name));
    }

    @Override
    public Iterable<? extends ColumnMetadata> getColumnMetadatas() {
        return columnMetadata;
    }

    @Override
    public Collection<String> getColumnNames() {
        return columnNames;
    }

    public int getColumnIndex(String name) {
        Integer index = columnIndexes.get(name.toLowerCase());
        if (index == null) {
            throw new NoSuchElementException("There is no column with the name " + name);
        }
        return index;
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class DB2ColumnMetadata implements ColumnMetadata {
        private final ColumnDescriptor columnDescriptor;

        @Override
        public String getName() {
            return columnDescriptor.name();
        }

        @Override
        public Class<?> getJavaType() {
            return mapType(columnDescriptor.jdbcType());
        }

        @Nullable
        private static Class<?> mapType(JDBCType type) {
            try {
                // https://vertx.io/docs/vertx-db2-client/java/#_db2_type_mapping
                // io.vertx.db2client.impl.drda.ClientTypes.mapDB2TypeToDriverType(...)
                Class<?> clazz = ClientTypes.preferredJavaType(type.getVendorTypeNumber());
                if (ByteBuf.class.equals(clazz)) {
                    return ByteBuffer.class; // R2DBC expects an NIO ByteBuffer, not a Netty ByteBuf
                } else {
                    return clazz;
                }
            } catch (IllegalArgumentException e) {
                log.warn("Unable to map JDBC Type: {}", type.getName(), e);
                return null;
            }
        }
    }
}
