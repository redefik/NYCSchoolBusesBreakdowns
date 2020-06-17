package it.uniroma2.dicii.sabd.dspproject.breakdowncausesbytimeslot;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownKafkaDeserializer;
import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownTimestampExtractor;
import it.uniroma2.dicii.sabd.dspproject.utils.KafkaStringSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.io.InputStream;
import java.util.Properties;

public class BreakdownCausesByTimeSlot {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Required args: <path/to/configuration/file>");
            System.exit(1);
        }

        /* Load configuration */
        Properties configuration = new Properties();
        InputStream configurationInputStream = BreakdownCausesByTimeSlot.class.getClassLoader().getResourceAsStream(args[0]);
        if (configurationInputStream != null) {
            configuration.load(configurationInputStream);
        } else {
            System.err.println("Cannot load configuration file");
            System.exit(2);
        }

        /* Flink environment setup */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /* Setting event-time */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Kafka configuration */
        Properties kafkaConsumerConfiguration = new Properties();
        kafkaConsumerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));
        kafkaConsumerConfiguration.setProperty("group.id", configuration.getProperty("breackdowncausesbytimeslot.kafka.consumer.groupid"));
        Properties kafkaProducerConfiguration = new Properties();
        kafkaProducerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));

        /* Kafka Consumer setup*/
        FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(configuration.getProperty("breackdowncausesbytimeslot.kafka.input.topic"), new BreakdownKafkaDeserializer(), kafkaConsumerConfiguration);
        /* Timestamp and watermark generation */
        breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());

        /* Kafka Producers setup */
        String dailyBreakdownCausesByTimeSlotKafkaTopic = configuration.getProperty("breackdowncausesbytimeslot.kafka.output.dailytopic");
        FlinkKafkaProducer<String> dailyBreakdownCausesByTimeSlotProducer =
                new FlinkKafkaProducer<>(dailyBreakdownCausesByTimeSlotKafkaTopic,
                        new KafkaStringSerializer(dailyBreakdownCausesByTimeSlotKafkaTopic),
                        kafkaProducerConfiguration,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        String weeklyBreakdownCausesByTimeSlotKafkaTopic = configuration.getProperty("breackdowncausesbytimeslot.kafka.output.weeklytopic");
        FlinkKafkaProducer<String> weeklyBreakdownCausesByTimeSlotProducer =
                new FlinkKafkaProducer<>(weeklyBreakdownCausesByTimeSlotKafkaTopic,
                        new KafkaStringSerializer(weeklyBreakdownCausesByTimeSlotKafkaTopic),
                        kafkaProducerConfiguration,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        /* Set Kafka consumer as stream source */
        DataStream<String> inputStream = env.addSource(breakdownsConsumer);

        KeyedStream<Tuple2<Tuple2<String, String>, Long>, Tuple> timeSlotCauses = inputStream
                /* Breakdown parsing: time slot and cause extraction */
                .flatMap(new TimeSlotAndCauseExtractor())
                /* Group by time slot and cause */
                .keyBy(0);

        /* Compute the occurrence of breakdowns caused by the same cause in the same time slot */
        timeSlotCauses
                .timeWindow(Time.hours(24))
                .reduce(new BreakdownCauseOccurencesCalculator())
                /* Calculate rank of causes for each time slot in a day*/
                .timeWindowAll(Time.hours(24))
                .process(new CausesRankByTimeSlotCalculator())
                .addSink(dailyBreakdownCausesByTimeSlotProducer);

        timeSlotCauses
                .timeWindow(Time.days(7))
                .reduce(new BreakdownCauseOccurencesCalculator())
                /* Calculate rank of causes for each time slot in a day*/
                .timeWindowAll(Time.days(7))
                .process(new CausesRankByTimeSlotCalculator())
                .addSink(weeklyBreakdownCausesByTimeSlotProducer);

        env.execute("Breakdown Causes By Time Slot");
    }
}
