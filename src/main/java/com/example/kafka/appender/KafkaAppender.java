package com.example.kafka.appender;

import com.example.kafka.appender.config.KafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;

@Plugin(name = "KafkaAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class KafkaAppender extends AbstractAppender {
    private final String topic;
    private final String bootstrapServers;
    private Producer<String, String> producer;

    protected KafkaAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                            boolean ignoreExceptions, String topic, String bootstrapServers) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        initializeProducer();
    }

    private void initializeProducer() {
        this.producer = new KafkaProducer<>(KafkaProducerConfig.getDefaultProperties(bootstrapServers));
    }

    @Override
    public void append(LogEvent event) {
        String message = new String(getLayout().toByteArray(event));
        producer.send(new ProducerRecord<>(topic, message));
    }

    @Override
    public void stop() {
        super.stop();
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginAttribute("ignoreExceptions") boolean ignoreExceptions) {

        return new KafkaAppender(
                name,
                filter,
                layout,
                ignoreExceptions,
                topic,
                bootstrapServers
        );
    }
}