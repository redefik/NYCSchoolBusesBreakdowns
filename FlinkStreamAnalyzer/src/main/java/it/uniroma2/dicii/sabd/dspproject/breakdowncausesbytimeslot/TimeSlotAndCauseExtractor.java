package it.uniroma2.dicii.sabd.dspproject.breakdowncausesbytimeslot;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.*;

public class TimeSlotAndCauseExtractor implements FlatMapFunction<String, Tuple2<Tuple2<String, String>, Long>>{

    @Override
    public void flatMap(String breakdownEvent, Collector<Tuple2<Tuple2<String, String>, Long>> collector) {

        try {
            String[] breakdownEventFields = BreakdownParser.getFieldsFromCsvString(breakdownEvent);

            /* Parsing time slot */
            String eventTimestampString = breakdownEventFields[TIMESTAMP_FIELD];
            String timeSlot = BreakdownParser.getTimeSlot(eventTimestampString);
            if (timeSlot == null){
                return;
            }

            /* Parsing breakdown cause */
            String cause = breakdownEventFields[CAUSE_FIELD];
            if (cause.equals("")){
                return;
            }

            collector.collect(new Tuple2<>(new Tuple2<>(timeSlot, cause), 1L));

        } catch (BreakdownParserException e) {
            e.printStackTrace();
        }
    }
}
