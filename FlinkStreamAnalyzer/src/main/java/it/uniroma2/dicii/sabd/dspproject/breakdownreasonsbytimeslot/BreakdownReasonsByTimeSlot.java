/*
* This class implements a Flink Streaming job used to answer the following query:
* Obtain the three delay reasons with the highest frequency considering the time slots
* 5:00-11:59 and 12:00-19:00. The statistics must be computed in two windows:
* 24 hours (event time)
* 7 days (event time)
* */

package it.uniroma2.dicii.sabd.dspproject.breakdownreasonsbytimeslot;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownKafkaDeserializer;
import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownTimestampExtractor;
import it.uniroma2.dicii.sabd.dspproject.utils.KafkaStringProducerFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

public class BreakdownReasonsByTimeSlot {

    /* This method defines the portion of topology used to compute delay reasons by time slot during a time window */
    private static void computeBreakdownReasonsByTimeSlot(KeyedStream<Tuple2<Tuple2<String, String>, Long>, Tuple> inputStream, Time windowSize, FlinkKafkaProducer<String> outputProducer, String outputName) {
        inputStream
            .timeWindow(windowSize)
            .reduce(new BreakdownReasonOccurrencesCalculator())
            /* Calculate rank of reasons for each time slot in the time window*/
            .timeWindowAll(windowSize)
            .process(new ReasonsRankByTimeSlotCalculator(outputName))
            /* Set Kafka producer as stream sink */
            .addSink(outputProducer);
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Required args: <path/to/configuration/file> <execution_mode>");
            System.exit(1);
        }

        /* Load configuration */
        Properties configuration = new Properties();
        try (InputStream configurationInputStream = new FileInputStream(args[0])) {
            configuration.load(configurationInputStream);
        } catch (FileNotFoundException e) {
            System.err.println("Cannot load configuration file");
            System.exit(2);
        }

        /* Flink environment setup */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* Latency tracking is set only in debug mode, since it affects negatively the overall performance */
        if (args[1].equals("debug")) {
            env.getConfig().setLatencyTrackingInterval(5);
        }

        /* Setting event-time */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Kafka configuration */
        Properties kafkaConsumerConfiguration = new Properties();
        kafkaConsumerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));
        kafkaConsumerConfiguration.setProperty("group.id", configuration.getProperty("breakdownreasonsbytimeslot.kafka.consumer.groupid"));
        Properties kafkaProducerConfiguration = new Properties();
        kafkaProducerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));

        /* Kafka Consumer setup*/
        FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(configuration.getProperty("commons.kafka.input.topic"), new BreakdownKafkaDeserializer(), kafkaConsumerConfiguration);
        /* Timestamp and watermark generation */
        breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());

        /* Kafka Producers setup */
        KafkaStringProducerFactory factory = new KafkaStringProducerFactory();

        String dailyBreakdownReasonsByTimeSlotKafkaTopic = configuration.getProperty("breakdownreasonsbytimeslot.kafka.output.dailytopic");
        FlinkKafkaProducer<String> dailyBreakdownReasonsByTimeSlotProducer = factory.createKafkaStringProducer(dailyBreakdownReasonsByTimeSlotKafkaTopic, kafkaProducerConfiguration);

        String weeklyBreakdownReasonsByTimeSlotKafkaTopic = configuration.getProperty("breakdownreasonsbytimeslot.kafka.output.weeklytopic");
        FlinkKafkaProducer<String> weeklyBreakdownReasonsByTimeSlotProducer = factory.createKafkaStringProducer(weeklyBreakdownReasonsByTimeSlotKafkaTopic, kafkaProducerConfiguration);

        /* Set Kafka consumer as stream source */
        DataStream<String> inputStream = env.addSource(breakdownsConsumer);

        KeyedStream<Tuple2<Tuple2<String, String>, Long>, Tuple> timeSlotReasons = inputStream
                /* Breakdown parsing: time slot and reason extraction */
                .flatMap(new TimeSlotAndReasonExtractor())
                /* Group by time slot and reason */
                .keyBy(0);

        /* Compute the occurrence of breakdowns caused by the same reason in the same time slot during the last 24 hours and 7 days */
        computeBreakdownReasonsByTimeSlot(timeSlotReasons, Time.hours(24), dailyBreakdownReasonsByTimeSlotProducer, "breakdownReasonsByTimeSlot24h");
        computeBreakdownReasonsByTimeSlot(timeSlotReasons, Time.days(7), weeklyBreakdownReasonsByTimeSlotProducer, "breakdownReasonsByTimeSlot7d");

        env.execute("Breakdown Reasons By Time Slot");
    }
}
