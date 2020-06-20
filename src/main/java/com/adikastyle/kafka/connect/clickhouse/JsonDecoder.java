package com.adikastyle.kafka.connect.clickhouse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.util.Collection;
import java.util.stream.Collectors;

public class JsonDecoder implements Decoder {

    private final Gson gson;

    public JsonDecoder() {
        gson = new GsonBuilder()
                .create();
    }

    @Override
    public Collection<Decodeable> decode(Collection<String> stringValue) {
        return stringValue
                .stream()
                .map(str -> gson.fromJson(str, JsonObject.class))
                .map(json -> (Decodeable) key -> json.get(key))
                .collect(Collectors.toList());
    }
}
