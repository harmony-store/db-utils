package com.harmonyplugins.dbutils.sql;

import com.harmonyplugins.dbutils.model.Database;
import com.harmonyplugins.errorreporter.Try;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class SQLDatabase implements Database<Connection> {
    private static final String JDBC_URL = "jdbc:%s://%s:%s/%s";

    private Connection connection;

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public void connect(Properties properties) {
        final String url = String.format(JDBC_URL,
            properties.getProperty("dbms"),
            properties.getProperty("hostname"),
            properties.getProperty("port"),
            properties.getProperty("database")
        );

        this.connection = Try.ofSupplier(() -> DriverManager.getConnection(
            url,
            properties.getProperty("username"),
            properties.getProperty("password")
        )).printStackTrace().or(null);
    }

    @Override
    public void disconnect() {
        Try.of(() -> {
           if(connection != null) connection.close();
        }).printStackTrace().run();
    }
}
