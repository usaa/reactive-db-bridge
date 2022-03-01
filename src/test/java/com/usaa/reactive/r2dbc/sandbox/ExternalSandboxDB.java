package com.usaa.reactive.r2dbc.sandbox;

import java.util.Map;

public class ExternalSandboxDB extends SandboxDB {
    public static boolean areEnvironmentVarsPresent() {
        Map<String, String> env = System.getenv();
        return env.containsKey("DB2_HOST") &&
                env.containsKey("DB2_PORT") &&
                env.containsKey("DB2_DATABASE") &&
                env.containsKey("DB2_USERNAME") &&
                env.containsKey("DB2_PASSWORD");
    }

    @Override
    public String getHost() { return System.getenv("DB2_HOST"); }
    @Override
    public int getPort() { return Integer.parseInt(System.getenv("DB2_PORT")); }
    @Override
    public String getDatabase() { return System.getenv("DB2_DATABASE"); }
    @Override
    public String getUsername() { return System.getenv("DB2_USERNAME"); }
    @Override
    public String getPassword() { return System.getenv("DB2_PASSWORD"); }
}
