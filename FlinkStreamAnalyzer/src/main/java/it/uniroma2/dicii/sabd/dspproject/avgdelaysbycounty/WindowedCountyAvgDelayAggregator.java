package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.EVENT_TIME_FORMAT;

/*
* Merges the average bus delays registered for several counties during a specific time window
* */
public class WindowedCountyAvgDelayAggregator extends ProcessAllWindowFunction<WindowedCountyAvgDelay, String, TimeWindow> {
    @Override
    public void process(Context context, Iterable<WindowedCountyAvgDelay> iterable, Collector<String> collector) {
        long startTimestamp = context.window().getStart();
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
        String printableStartTimestamp = sdf.format(date);
        StringBuilder output = new StringBuilder(printableStartTimestamp + "");
        for (WindowedCountyAvgDelay windowedCountyAvgDelay : iterable) {
            output.append(",").append(windowedCountyAvgDelay.getCounty()).append(",").append(windowedCountyAvgDelay.getAvgDelay());
        }
        collector.collect(output.toString());
    }
}
