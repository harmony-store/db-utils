package com.harmonyplugins.dbutils.sql;

import be.bendem.sqlstreams.Sql;
import com.harmonyplugins.dbutils.model.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

        try {
            this.connection = DriverManager.getConnection(
                url,
                properties.getProperty("username"),
                properties.getProperty("password")
            );

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void disconnect() {
        if(!isValid()) return;

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isValid() {
        if(connection == null) return false;

        try {
            return connection.isValid(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }
}
