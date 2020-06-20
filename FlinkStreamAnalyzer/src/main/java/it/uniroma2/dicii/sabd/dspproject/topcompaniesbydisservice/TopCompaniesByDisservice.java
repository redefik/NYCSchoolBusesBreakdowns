/*
 * This class implements a Flink Streaming job used to answer the following query:
 * Determine in real-time the 5 bus vendors with the highest disservice score using the
 * following time window:
 * 24 hours (event time)
 * 7 days (event time)
* */

package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import it.uniroma2.dicii.sabd.dspproject.utils.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TopCompaniesByDisservice {

    /*
    * Returns a list of school bus companies downloaded from NYC Education Department website
    * The first field of the tuple represents a pattern (used in event breakdown parsing)
    * The second field of the tuple is the extended name of the company
    *  */
    private static List<Tuple2<String, String>> loadSchoolBusCompanies(String companiesFilePath) {
        List<Tuple2<String, String>> companies = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(companiesFilePath))) {
            bufferedReader.readLine(); /* skip header */
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] fields = BreakdownParser.getFieldsFromCsvString(line);
                companies.add(new Tuple2<>(fields[0], fields[1]));
            }
            return companies;
        } catch (IOException | BreakdownParser.BreakdownParserException e) {
            return null;
        }
    }

    private static void computeTopKCompaniesByDisservice(KeyedStream<Tuple3<Double, String, String>, String> inputStream, Time windowSize, FlinkKafkaProducer<String> outputProducer, String outputName, int k) {
        inputStream
                /* Compute a disservice score for each company during time window */
                .timeWindow(windowSize)
                .aggregate(new CompanyDisserviceScoreCalculator(), new WindowedCompanyDisserviceScoreCalculator())
                /* Compute the k companies with the highest disservice score */
                .timeWindowAll(windowSize)
                .process(new TopKCompaniesByDisserviceScoreCalculator(outputName, k))
                /* Set Kafka producer as stream sink */
                .addSink(outputProducer);
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Required args: <path/to/configuration/file> <path/to/bus/companies/file> <execution_mode>");
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
        /* Load school bus companies */
        List<Tuple2<String, String>> schoolBusCompaniesPatterns = loadSchoolBusCompanies(args[1]);
        if (schoolBusCompaniesPatterns == null) {
            System.err.println("Cannot load school bus companies file");
            System.exit(3);
        }

        /* Flink environment setup */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* Latency tracking is set only in debug mode, since it affects negatively the overall performance */
        if (args[2].equals("debug")) {
            env.getConfig().setLatencyTrackingInterval(5);
        }

        /* Setting event-time */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Kafka configuration */
        Properties kafkaConsumerConfiguration = new Properties();
        kafkaConsumerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));
        kafkaConsumerConfiguration.setProperty("group.id", configuration.getProperty("topcompaniesbydisservice.kafka.consumer.groupid"));
        Properties kafkaProducerConfiguration = new Properties();
        kafkaProducerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));

        /* Kafka Consumer setup*/
        FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(configuration.getProperty("commons.kafka.input.topic"), new BreakdownKafkaDeserializer(), kafkaConsumerConfiguration);
        /* Timestamp and watermark generation */
        breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());

        /* Kafka Producers setup */
        KafkaStringProducerFactory factory = new KafkaStringProducerFactory();

        String dailyTopCompaniesByDisserviceKafkaTopic = configuration.getProperty("topcompaniesbydisservice.kafka.output.dailytopic");
        FlinkKafkaProducer<String> dailyTopCompaniesByDisserviceProducer = factory.createKafkaStringProducer(dailyTopCompaniesByDisserviceKafkaTopic, kafkaProducerConfiguration);

        String weeklyTopCompaniesByDisserviceKafkaTopic = configuration.getProperty("topcompaniesbydisservice.kafka.output.weeklytopic");
        FlinkKafkaProducer<String> weeklyTopCompaniesByDisserviceProducer = factory.createKafkaStringProducer(weeklyTopCompaniesByDisserviceKafkaTopic, kafkaProducerConfiguration);

        int topCompaniesRankingSize = Integer.parseInt(configuration.getProperty("topcompaniesbydisservice.ranking.size"));

        /* Set Kafka consumer as stream source */
        DataStream<String> inputStream = env.addSource(breakdownsConsumer);

        KeyedStream<Tuple3<Double, String, String>, String> delayAndReasonByCompany = inputStream
                /* Breakdown parsing: delay, reason and school bus company extraction */
                .flatMap(new DelayReasonCompanyExtractor(schoolBusCompaniesPatterns))
                /* Group by school bus company */
                .keyBy(x -> x.f2);

        /* Compute the worst bus companies by disservice during the last 24 hours, 7 days */
        computeTopKCompaniesByDisservice(delayAndReasonByCompany, Time.hours(24), dailyTopCompaniesByDisserviceProducer, "topCompaniesByDisservice24h", topCompaniesRankingSize );
        computeTopKCompaniesByDisservice(delayAndReasonByCompany, Time.days(7), weeklyTopCompaniesByDisserviceProducer, "topCompaniesByDisservice7d", topCompaniesRankingSize );

        env.execute("Top Companies By Disservice");

    }

}
