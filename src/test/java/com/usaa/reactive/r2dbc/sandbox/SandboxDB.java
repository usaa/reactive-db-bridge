package com.usaa.reactive.r2dbc.sandbox;

import com.usaa.reactive.r2dbc.db2.DB2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactoryOptions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Slf4j
public abstract class SandboxDB {
    @SneakyThrows
    public static SandboxDB getInstance() {
        log.info("Checking whether DB2_* environment variables are set");
        if (ExternalSandboxDB.areEnvironmentVarsPresent()) {
            log.info("DB2_* environment variables found, using external DB for tests");
            return new ExternalSandboxDB();
        } else {
            log.info("DB2_* environment variables not found, using testcontainers DB for tests");
            return new TestContainersDB();
        }
    }

    public void startDB() {
        // Nothing to do...
    }

    public void stopDB() {
        // Nothing to do...
    }

    public String getJdbcUrl() {
        return String.format("jdbc:db2://%s:%d/%s", getHost(), getPort(), getDatabase());
    }

    public ConnectionFactoryOptions getConnectionFactoryOptions() {
        return ConnectionFactoryOptions.builder()
                .option(DRIVER, DB2ConnectionFactoryProvider.DB2_DRIVER)
                .option(HOST, getHost())
                .option(PORT, getPort())
                .option(USER, getUsername())
                .option(PASSWORD, getPassword())
                .option(DATABASE, getDatabase())
                .build();
    }

    public abstract String getHost();

    public abstract int getPort();

    public abstract String getDatabase();

    public abstract String getUsername();

    public abstract String getPassword();
}
