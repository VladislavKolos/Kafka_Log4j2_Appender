package com.example.kafka.appender.config;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@UtilityClass
public final class KafkaProducerConfig {
    private static final String DEFAULT_ACKS = "0";

    public static Properties getDefaultProperties(String bootstrapServers) {
        var props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);

        return props;
    }
}