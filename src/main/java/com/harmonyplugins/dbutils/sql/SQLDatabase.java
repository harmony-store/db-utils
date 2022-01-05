package com.harmonyplugins.dbutils.sql;

import com.harmonyplugins.dbutils.model.Database;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLDatabase implements Database<Connection> {
    private static final AtomicInteger POOL_COUNTER = new AtomicInteger(0);

    private static final int MAXIMUM_POOL_SIZE = (Runtime.getRuntime().availableProcessors() * 2) + 1;
    private static final int MINIMUM_IDLE = Math.min(MAXIMUM_POOL_SIZE, 10);

    private static final long MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30);
    private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    private static final long LEAK_DETECTION_THRESHOLD = TimeUnit.SECONDS.toMillis(10);

    private static final String JDBC_URL = "jdbc:%s://%s:%s/%s";

    private HikariDataSource source;

    public HikariDataSource getSource() {
        return source;
    }

    @Override
    public Connection getConnection() {
        try {
            return source.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void connect(Properties properties) {
        final String url = String.format(JDBC_URL,
            properties.getProperty("dbms"),
            properties.getProperty("hostname"),
            properties.getProperty("port"),
            properties.getProperty("database")
        );

        final HikariConfig hikari = new HikariConfig();

        hikari.setPoolName("helper-sql-" + POOL_COUNTER.getAndIncrement());

        hikari.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikari.setJdbcUrl(url);

        hikari.setUsername(properties.getProperty("username"));
        hikari.setPassword(properties.getProperty("password"));

        hikari.setMaximumPoolSize(MAXIMUM_POOL_SIZE);
        hikari.setMinimumIdle(MINIMUM_IDLE);

        hikari.setMaxLifetime(MAX_LIFETIME);
        hikari.setConnectionTimeout(CONNECTION_TIMEOUT);
        hikari.setLeakDetectionThreshold(LEAK_DETECTION_THRESHOLD);

        Map<String, String> map = new HashMap<String, String>() {{
            put("useUnicode", "true");
            put("characterEncoding", "utf8");

            put("cachePrepStmts", "true");
            put("prepStmtCacheSize", "250");
            put("prepStmtCacheSqlLimit", "2048");
            put("useServerPrepStmts", "true");
            put("useLocalSessionState", "true");
            put("rewriteBatchedStatements", "true");
            put("cacheResultSetMetadata", "true");
            put("cacheServerConfiguration", "true");
            put("elideSetAutoCommits", "true");
            put("maintainTimeStats", "false");
            put("alwaysSendSetIsolation", "false");
            put("cacheCallableStmts", "true");

            put("socketTimeout", String.valueOf(TimeUnit.SECONDS.toMillis(30)));
        }};

        for (Map.Entry<String, String> property : map.entrySet()) {
            hikari.addDataSourceProperty(property.getKey(), property.getValue());
        }

        this.source = new HikariDataSource(hikari);
    }

    @Override
    public void disconnect() {
        if (!isValid()) return;

        source.close();
    }

    @Override
    public boolean isValid() {
        if (source == null) return false;

        return source.isRunning();
    }
}
