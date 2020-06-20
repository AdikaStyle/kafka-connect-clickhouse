package com.adikastyle.kafka.connect.clickhouse;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Module {
    private final ClickhouseRepository clickhouseRepository;
    private Map<String, String> config;
    private final ClickhouseService clickhouseService;
    private final Decoder decoder;

    public Module(Map<String, String> config) throws Exception {
        this.config = config;
        clickhouseRepository = new ClickhouseRepositoryImpl(config);
        decoder = new JsonDecoder();
        clickhouseService =  new ClickhouseServiceImpl(
                2048,
                clickhouseRepository,
                decoder,
                100, 5
                );
    }

    public void monitorSchemaChanges() {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            try {
                clickhouseService.refreshTablesMetadata();
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    public void dispose() throws Exception {
        this.clickhouseRepository.dispose();
    }

    public ClickhouseService getClickhouseService() {
        return clickhouseService;
    }

    public Map<String, String> getConfig() {
        return config;
    }
}
