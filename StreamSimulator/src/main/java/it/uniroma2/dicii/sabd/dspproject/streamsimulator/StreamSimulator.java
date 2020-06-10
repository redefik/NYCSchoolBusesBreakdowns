package it.uniroma2.dicii.sabd.dspproject.streamsimulator;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*
* This class replays a dataset about NYC School Buses breakdowns to simulate a data stream.
* The temporal scale is accelerated: one minute in event time corresponds to one millisecond in simulation time.
* The stream is published to a configurable Kafka topic.
* */


public class StreamSimulator {

    private static final String eventTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.sss";

    /* This function parses the timestamp of a breakdown event returning a corresponding Date object */
    private static Date getEventTime(String breakdownEvent) throws ParseException, IOException, CsvValidationException {
        CSVParserBuilder csvParserBuilder = new CSVParserBuilder().withSeparator(';');
        CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new StringReader(breakdownEvent)).withCSVParser(csvParserBuilder.build());
        CSVReader csvReader = csvReaderBuilder.build();
        String[] fields = csvReader.readNext();
        csvReader.close();
        String stringDate = fields[7];
        SimpleDateFormat sdf = new SimpleDateFormat(eventTimeFormat, Locale.US);
        return sdf.parse(stringDate);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException, CsvValidationException {

        if (args.length != 3) {
            System.err.println("Required arguments: input file, kafka address, kafka topic");
            System.exit(1);
        }

        /* Kafka configuration */
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", args[1]);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProperties);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]));
        bufferedReader.readLine(); /* skip header */
        String firstLine = bufferedReader.readLine();
        String secondLine;
        while ((secondLine = bufferedReader.readLine()) != null) {
            /* Publish to Kafka */
            producer.send(new ProducerRecord<String, String>(args[2], String.valueOf(System.currentTimeMillis()),firstLine));
            Date firstDate = getEventTime(firstLine);
            Date secondDate = getEventTime(secondLine);
            long diffInMillies = secondDate.getTime() - firstDate.getTime();
            long diffInMinutes = TimeUnit.MINUTES.convert(diffInMillies, TimeUnit.MILLISECONDS);
            /* Simulate delay */
            Thread.sleep(diffInMinutes);
            firstLine = secondLine;
        }
        producer.send(new ProducerRecord<String, String>(args[2], String.valueOf(System.currentTimeMillis()), firstLine));
        producer.close();
        bufferedReader.close();

    }

}
