package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.EVENT_TIME_FORMAT;

/*
* This class obtains the K companies with the highest disservice score during a time window
* */
public class TopKCompaniesByDisserviceScoreCalculator extends ProcessAllWindowFunction<WindowedCompanyDisserviceScore, String, TimeWindow> {

    private int k;
    private transient Meter avgThroughput;
    private String operatorName;

    /* Instrumentation code for performance evaluation */
    @Override
    public void open(Configuration parameters) {
        com.codahale.metrics.Meter dropWizardMeter = new com.codahale.metrics.Meter();
        this.avgThroughput =
                getRuntimeContext()
                        .getMetricGroup()
                        .meter(operatorName + "AvgThroughput", new DropwizardMeterWrapper(dropWizardMeter));
    }


    public TopKCompaniesByDisserviceScoreCalculator(String operatorName, int k) {
        super();
        this.operatorName = operatorName;
        this.k = k;
    }

    @Override
    public void process(Context context, Iterable<WindowedCompanyDisserviceScore> iterable, Collector<String> collector) {
        /* Sort companies by disservice score */
        List<WindowedCompanyDisserviceScore> companyDisserviceScores = new ArrayList<>();
        for (WindowedCompanyDisserviceScore windowedCompanyDisserviceScore : iterable) {
            companyDisserviceScores.add(windowedCompanyDisserviceScore);
        }
        Comparator<WindowedCompanyDisserviceScore> companyDisserviceScoreComparator = new WindowedCompanyDisserviceScoreComparator();
        companyDisserviceScores.sort(companyDisserviceScoreComparator.reversed());
        List<WindowedCompanyDisserviceScore> topKCompanies;
        if (companyDisserviceScores.size() > k) {
            topKCompanies = companyDisserviceScores.subList(0, k);
        } else {
            topKCompanies = companyDisserviceScores;
        }
        /* Get start timestamp of the window */
        long startTimestamp = context.window().getStart();
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
        String printableStartTimestamp = sdf.format(date);
        /* Build the output ranking string */
        StringBuilder output = new StringBuilder(printableStartTimestamp + "");
        for (WindowedCompanyDisserviceScore companyDisserviceScore : topKCompanies) {
            output.append(",").append(companyDisserviceScore.getCompany()).append(",").append(companyDisserviceScore.getDisserviceScore());
        }
        this.avgThroughput.markEvent();
        collector.collect(output.toString());
    }
}
