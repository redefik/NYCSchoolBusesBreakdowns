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
import it.uniroma2.dicii.sabd.dspproject.utils.KafkaStringSerializer;
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

public class AvgDelaysByCounty {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Required args: <path/to/configuration/file>");
			System.exit(1);
		}

		/* Load configuration */
		Properties configuration = new Properties();
		InputStream configurationInputStream = AvgDelaysByCounty.class.getClassLoader().getResourceAsStream(args[0]);
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
		kafkaConsumerConfiguration.setProperty("group.id", configuration.getProperty("avgdelaysbycounty.kafka.consumer.groupid"));
		Properties kafkaProducerConfiguration = new Properties();
		kafkaProducerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));

		/* Kafka Consumer setup */
		FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(configuration.getProperty("commons.kafka.input.topic"), new BreakdownKafkaDeserializer(), kafkaConsumerConfiguration);
		/* Timestamp and watermark generation */
		breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());

		/* Kafka Producers setup */
		String dailyAvgDelaysByCountyKafkatopic = configuration.getProperty("avgdelaysbycounty.kafka.output.dailytopic");
		FlinkKafkaProducer<String> dailyAvgDelaysByCountyProducer =
				new FlinkKafkaProducer<>(dailyAvgDelaysByCountyKafkatopic,
						new KafkaStringSerializer(dailyAvgDelaysByCountyKafkatopic),
						kafkaProducerConfiguration,
						FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
		String weeklyAvgDelaysByCountyKafkaTopic = configuration.getProperty("avgdelaysbycounty.kafka.output.weeklytopic");
		FlinkKafkaProducer<String> weeklyAvgDelaysByCountyProducer =
				new FlinkKafkaProducer<>(weeklyAvgDelaysByCountyKafkaTopic,
						new KafkaStringSerializer(weeklyAvgDelaysByCountyKafkaTopic),
						kafkaProducerConfiguration,
						FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
		String monthlyAvgDelaysByCountyKafkaTopic = configuration.getProperty("avgdelaysbycounty.kafka.output.monthlytopic");
		FlinkKafkaProducer<String> monthlyAvgDelaysByCountyProducer =
				new FlinkKafkaProducer<>(monthlyAvgDelaysByCountyKafkaTopic,
						new KafkaStringSerializer(monthlyAvgDelaysByCountyKafkaTopic),
						kafkaProducerConfiguration,
						FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		/* Set Kafka consumer as stream source */
		DataStream<String> inputStream = env.addSource(breakdownsConsumer);

		KeyedStream<Tuple2<String, Double>, String> countyDelays = inputStream
				/* Breakdown parsing: county and delay extraction */
				.flatMap(new CountyAndDelayExtractor())
				/* Group by county */
				.keyBy(x-> x.f0);


		countyDelays
				/* Compute average delay by county during the last 24 hours */
				.timeWindow(Time.hours(24))
				.aggregate(new AvgDelayCalculator(), new WindowedCountyAvgDelayCalculator())
				/* Merge computed delays into a single output */
				.timeWindowAll(Time.hours(24))
				.process(new WindowedCountyAvgDelayAggregator())
				/* Set Kafka producer as stream sink */
				.addSink(dailyAvgDelaysByCountyProducer);

		countyDelays
				/* Compute average delay by county during the last 7 days */
				.timeWindow(Time.days(7))
				.aggregate(new AvgDelayCalculator(), new WindowedCountyAvgDelayCalculator())
				/* Merge computed delays into a single output */
				.timeWindowAll(Time.days(7))
				.process(new WindowedCountyAvgDelayAggregator())
				/* Set Kafka producer as stream sink */
				.addSink(weeklyAvgDelaysByCountyProducer);

		countyDelays
				/* Compute average delay by county during the last 30 days */
				.timeWindow(Time.days(30))
				.aggregate(new AvgDelayCalculator(), new WindowedCountyAvgDelayCalculator())
				/* Merge computed delays into a single output */
				.timeWindowAll(Time.days(30))
				.process(new WindowedCountyAvgDelayAggregator())
				/* Set Kafka producer as stream sink */
				.addSink(monthlyAvgDelaysByCountyProducer);

		env.execute("Average Delays By County");
	}
}
