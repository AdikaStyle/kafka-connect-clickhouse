package com.adikastyle.kafka.connect.clickhouse;

import java.util.Collection;

public interface Decoder {
    Collection<Decodeable> decode(Collection<String> stringValue);

    interface Decodeable {
        Object get(String key);
    }
}
