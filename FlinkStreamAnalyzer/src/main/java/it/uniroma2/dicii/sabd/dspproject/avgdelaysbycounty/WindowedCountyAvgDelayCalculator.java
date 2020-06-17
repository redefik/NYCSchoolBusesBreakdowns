package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
* This class enriches the average delay registered in a county during
* a time window with the name of the county. (Used in combination with AvgDelayCalculator)
* */
public class WindowedCountyAvgDelayCalculator extends ProcessWindowFunction<Double, WindowedCountyAvgDelay , String, TimeWindow> {

    @Override
    public void process(String county, Context context, Iterable<Double> iterable, Collector<WindowedCountyAvgDelay> collector) {
        WindowedCountyAvgDelay windowedCountyAvgDelay = new WindowedCountyAvgDelay();
        windowedCountyAvgDelay.setCounty(county);
        windowedCountyAvgDelay.setAvgDelay(iterable.iterator().next());
        collector.collect(windowedCountyAvgDelay);
    }
}
