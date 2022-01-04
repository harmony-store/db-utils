package com.harmonyplugins.dbutils.model;

public interface Repository<K, V> {
    V find(K key);

    void insert(K key , V value);

    void update(K key, V value);

    void delete(K key);
}
