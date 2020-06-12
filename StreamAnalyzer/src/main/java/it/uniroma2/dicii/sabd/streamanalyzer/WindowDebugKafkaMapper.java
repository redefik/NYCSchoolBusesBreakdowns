package it.uniroma2.dicii.sabd.streamanalyzer;

import it.uniroma2.dicii.sabd.streamanalyzer.boroughdelaystopology.BoroughDelaySummary;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

public class WindowDebugKafkaMapper implements TupleToKafkaMapper<String, String> {
    @Override
    public String getKeyFromTuple(Tuple tuple) {
        return String.valueOf(System.currentTimeMillis());
    }

    @Override
    public String getMessageFromTuple(Tuple tuple) {
        long rawTimestamp = tuple.getLongByField("startTimestamp");
        HashMap<String, BoroughDelaySummary> hm = (HashMap<String, BoroughDelaySummary>) tuple.getValueByField("avgDelayByBorough");
        Date date = new Date(rawTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
        String printableTimestamp = sdf.format(date);
        return printableTimestamp + "," + hm.toString();
    }
}
