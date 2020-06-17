package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.COUNTY_FIELD;
import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.DELAY_FIELD;

/*
* Parses a breakdown event extracting the county into which the event occurred and the delay caused by the breakdown
* */
public class CountyAndDelayExtractor implements FlatMapFunction<String, Tuple2<String, Double>> {
    @Override
    public void flatMap(String breakdownEvent, Collector<Tuple2<String, Double>> collector) {
        try {
            String[] breakdownEventFields = BreakdownParser.getFieldsFromCsvString(breakdownEvent);
            /* Parsing county */
            String county = breakdownEventFields[COUNTY_FIELD];
            if (county.equals("")) {
                return;
            }
            /* Parsing delay */
            String delayString = breakdownEventFields[DELAY_FIELD];
            Double delay = BreakdownParser.parseDelay(delayString);
            if (delay == null) {
                return;
            }
            collector.collect(new Tuple2<>(county, delay));

        } catch (BreakdownParser.BreakdownParserException e) {
            e.printStackTrace();
        }
    }
}
