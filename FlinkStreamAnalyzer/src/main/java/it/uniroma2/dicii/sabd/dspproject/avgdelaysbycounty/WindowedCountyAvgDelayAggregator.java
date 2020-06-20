package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
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

    private transient Meter avgThroughput;
    private String operatorName;

    public WindowedCountyAvgDelayAggregator(String operatorName) {
        super();
        this.operatorName = operatorName;
    }

    /* Instrumentation code for performance evaluation */
    @Override
    public void open(Configuration parameters) {
        com.codahale.metrics.Meter dropWizardMeter = new com.codahale.metrics.Meter();
        this.avgThroughput =
                getRuntimeContext()
                        .getMetricGroup()
                        .meter(operatorName + "AvgThroughput", new DropwizardMeterWrapper(dropWizardMeter));
    }

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
        this.avgThroughput.markEvent();
        collector.collect(output.toString());
    }
}
