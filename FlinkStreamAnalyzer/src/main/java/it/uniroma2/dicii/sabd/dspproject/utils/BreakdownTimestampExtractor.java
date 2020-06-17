package it.uniroma2.dicii.sabd.dspproject.utils;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/*
* This class is used to extract an event-time timestamp from a breakdown event record
* */
public class BreakdownTimestampExtractor extends AscendingTimestampExtractor<String> {

    @Override
    public long extractAscendingTimestamp(String breakdownEventString) {
        String rawTimestampString = breakdownEventString.substring(0, breakdownEventString.indexOf(';'));
        return Long.parseLong(rawTimestampString);
    }

}
