package com.adikastyle.kafka.connect.clickhouse;

import org.apache.kafka.common.cache.Cache;

import java.util.*;

public class LruCache<K, V> implements Cache<K, V> {
    private final LinkedHashMap<K, V> cache;

    public LruCache(final int maxSize) {
        this.cache = new LinkedHashMap<K, V>(16, 0.75F, true) {
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return this.size() > maxSize;
            }
        };
    }

    public V get(K key) {
        return this.cache.get(key);
    }

    public void put(K key, V value) {
        this.cache.put(key, value);
    }

    public final Collection<K> getKeysSnapshot() {
        final List<K> out = new LinkedList<K>();
        this.cache.forEach((k, v) -> {
            out.add(k);
        });
        return out;
    }

    public boolean remove(K key) {
        return this.cache.remove(key) != null;
    }

    public long size() {
        return (long)this.cache.size();
    }
}
