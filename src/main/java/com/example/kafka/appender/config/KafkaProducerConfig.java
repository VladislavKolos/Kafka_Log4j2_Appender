package com.example.kafka.appender.config;

import lombok.experimental.UtilityClass;

import java.util.Properties;

@UtilityClass
public final class KafkaProducerConfig {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String ACKS_CONFIG = "acks";
    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_ACKS = "0";

    public static Properties getDefaultProperties(String bootstrapServers) {
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS, bootstrapServers);
        props.put(KEY_SERIALIZER, STRING_SERIALIZER);
        props.put(VALUE_SERIALIZER, STRING_SERIALIZER);
        props.put(ACKS_CONFIG, DEFAULT_ACKS);
        return props;
    }
}