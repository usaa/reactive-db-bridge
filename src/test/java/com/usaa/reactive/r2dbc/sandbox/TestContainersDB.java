package com.usaa.reactive.r2dbc.sandbox;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.Db2Container;

@Slf4j
public class TestContainersDB extends SandboxDB {
    private static Db2Container db2;

    @Override
    public void startDB() {
        log.info("Starting DB2 via Testcontainers");
        db2 = new Db2Container().acceptLicense();
        db2.start();
        //System.out.println(db2.getLogs());
    }

    @Override
    public void stopDB() {
        if (db2 != null) {
            log.info("Stopping DB2 via Testcontainers");
            db2.stop();
        }
    }

    @Override
    public String getHost() {
        return db2.getHost();
    }
    @Override
    public int getPort() {
        return db2.getMappedPort(Db2Container.DB2_PORT);
    }
    @Override
    public String getDatabase() {
        return db2.getDatabaseName();
    }

    @Override
    public String getUsername() {
        return db2.getUsername();
    }

    @Override
    public String getPassword() {
        return db2.getPassword();
    }
}
