/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownKafkaDeserializer;
import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownTimestampExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class AvgDelaysByCounty {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Required args: <input kafka topic> <kafka address>");
			System.exit(1);
		}

		/* Flink environment setup */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		/* Setting event-time */
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		/* Kafka consumer setup */
		Properties kafkaConfiguration = new Properties();
		kafkaConfiguration.setProperty("bootstrap.servers", args[1]);
		kafkaConfiguration.setProperty("group.id", "avgDelaysByCounty");
		FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(args[0], new BreakdownKafkaDeserializer(), kafkaConfiguration);
		/* Timestamp and watermark generation */
		breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());
		/* Set Kafka consumer as stream source */
		DataStream<String> inputStream = env.addSource(breakdownsConsumer);


		inputStream
				/* Breakdown parsing: county and delay extraction */
				.flatMap(new CountyAndDelayExtractor())
				/* Group by county */
				.keyBy(x-> x.f0)
				/* Sliding window */
				.timeWindow(Time.hours(24), Time.hours(1))
				/* Compute average delay by window and county */
				.aggregate(new AvgDelayCalculator(), new WindowedCountyAvgDelayCalculator())
				/*.keyBy(WindowedCountyAvgDelay::getStartTimestamp)*/
				.timeWindowAll(Time.hours(24), Time.hours(1))
				.process(new WindowedCountyAvgDelayAggregator())
				.print();



		env.execute("Average Delays By County");
	}
}
