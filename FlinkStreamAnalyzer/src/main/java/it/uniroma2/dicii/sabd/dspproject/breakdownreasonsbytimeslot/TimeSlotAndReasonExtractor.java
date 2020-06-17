package it.uniroma2.dicii.sabd.dspproject.breakdownreasonsbytimeslot;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.*;

/*
 * Parses a breakdown event extracting the time slot into which the event occurred and the reason that caused the breakdown
 * Furthermore, it adds a field with value 1 used to perform the windowed computations
 * */
public class TimeSlotAndReasonExtractor implements FlatMapFunction<String, Tuple2<Tuple2<String, String>, Long>>{

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

            /* Parsing breakdown reason */
            String reason = breakdownEventFields[REASON_FIELD];
            if (reason.equals("")){
                return;
            }

            collector.collect(new Tuple2<>(new Tuple2<>(timeSlot, reason), 1L));

        } catch (BreakdownParserException e) {
            e.printStackTrace();
        }
    }
}
