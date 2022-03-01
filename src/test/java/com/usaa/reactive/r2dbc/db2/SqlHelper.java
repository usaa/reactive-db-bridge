package com.usaa.reactive.r2dbc.db2;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SqlHelper {
    public static void dropTables(DataSource dataSource) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            dropTable(c, "test");
            dropTable(c, "blob_test");
            dropTable(c, "clob_test");
            dropTable(c, "test_two_column");
        }
    }

    private static void dropTable(Connection c, String name) {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("DROP TABLE " + name);
        } catch (SQLException e) {
            if (-204 == e.getErrorCode() && "42704".equals(e.getSQLState())) {
                log.debug("No need to drop table {}, as it does not exist", name);
            } else {
                log.info("Unable to drop table {}", name, e);
            }
        }
    }
}
