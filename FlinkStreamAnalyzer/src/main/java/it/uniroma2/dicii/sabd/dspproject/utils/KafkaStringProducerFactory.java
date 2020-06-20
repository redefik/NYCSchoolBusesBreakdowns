package it.uniroma2.dicii.sabd.dspproject.utils;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaStringProducerFactory {

    public FlinkKafkaProducer<String> createKafkaStringProducer(String topic, Properties producerConfiguration) {
        return new FlinkKafkaProducer<>(topic, new KafkaStringSerializer(topic), producerConfiguration, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

}
