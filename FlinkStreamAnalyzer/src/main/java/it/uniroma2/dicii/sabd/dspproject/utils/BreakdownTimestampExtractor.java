package it.uniroma2.dicii.sabd.dspproject.utils;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class BreakdownTimestampExtractor extends AscendingTimestampExtractor<String> {

    @Override
    public long extractAscendingTimestamp(String breakdownEventString) {
        String rawTimestampString = breakdownEventString.substring(0, breakdownEventString.indexOf(';'));
        return Long.parseLong(rawTimestampString);
    }

}
