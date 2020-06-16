package it.uniroma2.dicii.sabd.dspproject.utils;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaStringSerializer implements KafkaSerializationSchema<String> {

    private String topic;

    public KafkaStringSerializer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String inputElement, @Nullable Long l) {
        return new ProducerRecord<>(topic, inputElement.getBytes(StandardCharsets.UTF_8));
    }
}
