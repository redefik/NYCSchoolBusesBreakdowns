package it.uniroma2.dicii.sabd.streamanalyzer.boroughdelaystopology;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import it.uniroma2.dicii.sabd.streamanalyzer.utils.EventBreakdownParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;


public class BoroughDelayExtractorBolt extends BaseRichBolt {

    private static final String EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.sss";
    private static final int EVENT_TIMESTAMP_FIELD = 7;
    private static final int BOROUGH_FIELD = 9;
    private static final int DELAY_FIELD = 11;

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            String breakdownEvent = tuple.getStringByField("value");
            // TODO refactor parsing to another class
            CSVParserBuilder csvParserBuilder = new CSVParserBuilder().withSeparator(';');
            CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new StringReader(breakdownEvent)).withCSVParser(csvParserBuilder.build());
            CSVReader csvReader = csvReaderBuilder.build();
            String[] fields = csvReader.readNext();
            csvReader.close();
            // TODO refactor...
            // parsing event time
            String eventTimestampString = fields[EVENT_TIMESTAMP_FIELD];
            SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
            Long eventTimestamp = sdf.parse(eventTimestampString).getTime();
            // parsing borough
            String borough = fields[BOROUGH_FIELD];
            if (borough.equals("")) {
                collector.ack(tuple);
                return;
            }
            // parsing delays
            // TODO refactor...
            String delayString = fields[DELAY_FIELD];
            Double delay = EventBreakdownParser.parseDelay(delayString);
            if (delay == null) {
                collector.ack(tuple);
                return;
            }
            collector.ack(tuple);
            collector.emit(tuple, new Values(eventTimestamp, borough, delay));

        } catch (CsvValidationException | IOException | ParseException e) {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ts","borough","delay"));
    }
}
