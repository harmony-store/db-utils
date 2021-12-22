package com.harmonyplugins.dbutils.model;

import java.util.Properties;

public interface Database<T> {
    T getConnection();

    void connect(Properties properties);

    void disconnect();
}
