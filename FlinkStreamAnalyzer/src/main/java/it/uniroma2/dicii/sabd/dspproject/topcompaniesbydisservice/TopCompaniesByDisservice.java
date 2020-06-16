package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownKafkaDeserializer;
import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser;
import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownTimestampExtractor;
import it.uniroma2.dicii.sabd.dspproject.utils.KafkaStringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Required args: <path/to/configuration/file> <path/to/bus/companies/file>");
            System.exit(1);
        }

        /* Load configuration */
        Properties configuration = new Properties();
        InputStream configurationInputStream = TopCompaniesByDisservice.class.getClassLoader().getResourceAsStream(args[0]);
        if (configurationInputStream != null) {
            configuration.load(configurationInputStream);
        } else {
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
        /* Setting event-time */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Kafka configuration */
        Properties kafkaConsumerConfiguration = new Properties();
        kafkaConsumerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));
        kafkaConsumerConfiguration.setProperty("group.id", configuration.getProperty("topcompaniesbydisservice.kafka.consumer.groupid"));
        Properties kafkaProducerConfiguration = new Properties();
        kafkaProducerConfiguration.setProperty("bootstrap.servers", configuration.getProperty("commons.kafka.address"));

        /* Kafka Consumer setup*/
        // TODO after merging transform avgdelaysbycounty to commons
        FlinkKafkaConsumer<String> breakdownsConsumer = new FlinkKafkaConsumer<>(configuration.getProperty("avgdelaysbycounty.kafka.input.topic"), new BreakdownKafkaDeserializer(), kafkaConsumerConfiguration);
        /* Timestamp and watermark generation */
        breakdownsConsumer.assignTimestampsAndWatermarks(new BreakdownTimestampExtractor());

        /* Kafka Producers setup */
        // TODO Possible refactoring after merging
        String dailyTopCompaniesByDisserviceKafkaTopic = configuration.getProperty("topcompaniesbydisservice.kafka.output.dailytopic");
        FlinkKafkaProducer<String> dailyTopCompaniesByDisserviceProducer =
                new FlinkKafkaProducer<>(dailyTopCompaniesByDisserviceKafkaTopic,
                        new KafkaStringSerializer(dailyTopCompaniesByDisserviceKafkaTopic),
                        kafkaProducerConfiguration,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        String weeklyTopCompaniesByDisserviceKafkaTopic = configuration.getProperty("topcompaniesbydisservice.kafka.output.weeklytopic");
        FlinkKafkaProducer<String> weeklyTopCompaniesByDisserviceProducer =
                new FlinkKafkaProducer<>(weeklyTopCompaniesByDisserviceKafkaTopic,
                        new KafkaStringSerializer(weeklyTopCompaniesByDisserviceKafkaTopic),
                        kafkaProducerConfiguration,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        /* Set Kafka consumer as stream source */
        DataStream<String> inputStream = env.addSource(breakdownsConsumer);

        inputStream
                /* Breakdown parsing: delay, reason and school bus company extraction */
                .flatMap(new DelayReasonCompanyExtractor(schoolBusCompaniesPatterns))
                .print();



        env.execute("Top Companies By Disservice");

    }

}
