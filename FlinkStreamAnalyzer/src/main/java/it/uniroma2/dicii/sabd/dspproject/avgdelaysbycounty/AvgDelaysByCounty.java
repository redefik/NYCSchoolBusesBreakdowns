/*
* This class implements a Flink Streaming job used to answer the following query:
* Determine in rel-time the average bus delay per county using the following windows:
* 24 hours (event time)
* 7 days (event time)
* 1 month (event time)
* */

package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownKafkaDeserializer;
import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownTimestampExtractor;
import it.uniroma2.dicii.sabd.dspproject.utils.KafkaStringProducerFactory;
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

public class AvgDelaysByCounty {

	/* This method defines the portion of topology used to compute average delay by county during a time window */
	private static void computeAverageDelayByCounty(KeyedStream<Tuple2<String, Double>, String> inputStream, Time windowSize, FlinkKafkaProducer<String> outputProducer, String outputName) {
		inputStream
				/* Compute average delay by county during the period represented by windowSize */
				.timeWindow(windowSize)
				.aggregate(new AvgDelayCalculator(), new WindowedCountyAvgDelayCalculator())
				/* Merge computed delays into a single output */
				.timeWindowAll(windowSize)
				.process(new WindowedCountyAvgDelayAggregator(outputName))
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
		kafkaConsumerConfiguration.setProperty("group.id", configuration.getProperty("avgdelaysbycounty.kafka.consumer.groupid"));
		Properties kafkaProducerConfiguration = new Properties();
		kafkaProducerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));

		/* Kafka Consumer setup */
		FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(configuration.getProperty("commons.kafka.input.topic"), new BreakdownKafkaDeserializer(), kafkaConsumerConfiguration);
		/* Timestamp and watermark generation */
		breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());

		/* Kafka Producers setup */
		KafkaStringProducerFactory factory = new KafkaStringProducerFactory();

		String dailyAvgDelaysByCountyKafkaTopic = configuration.getProperty("avgdelaysbycounty.kafka.output.dailytopic");
		FlinkKafkaProducer<String> dailyAvgDelaysByCountyProducer = factory.createKafkaStringProducer(dailyAvgDelaysByCountyKafkaTopic, kafkaProducerConfiguration);

		String weeklyAvgDelaysByCountyKafkaTopic = configuration.getProperty("avgdelaysbycounty.kafka.output.weeklytopic");
		FlinkKafkaProducer<String> weeklyAvgDelaysByCountyProducer = factory.createKafkaStringProducer(weeklyAvgDelaysByCountyKafkaTopic, kafkaProducerConfiguration);

		String monthlyAvgDelaysByCountyKafkaTopic = configuration.getProperty("avgdelaysbycounty.kafka.output.monthlytopic");
		FlinkKafkaProducer<String> monthlyAvgDelaysByCountyProducer = factory.createKafkaStringProducer(monthlyAvgDelaysByCountyKafkaTopic, kafkaProducerConfiguration);

		/* Set Kafka consumer as stream source */
		DataStream<String> inputStream = env.addSource(breakdownsConsumer);

		KeyedStream<Tuple2<String, Double>, String> countyDelays = inputStream
				/* Breakdown parsing: county and delay extraction */
				.flatMap(new CountyAndDelayExtractor())
				/* Group by county */
				.keyBy(x-> x.f0);

		/* Compute average delay by county during the last 24 hours, 7 days and 30 days */
		computeAverageDelayByCounty(countyDelays, Time.hours(24), dailyAvgDelaysByCountyProducer, "avgDelaysByCounty24h");
		computeAverageDelayByCounty(countyDelays, Time.days(7), weeklyAvgDelaysByCountyProducer, "avgDelaysByCounty7d");
		computeAverageDelayByCounty(countyDelays, Time.days(30), monthlyAvgDelaysByCountyProducer, "avgDelaysByCounty30d");

		env.execute("Average Delays By County");
	}
}
