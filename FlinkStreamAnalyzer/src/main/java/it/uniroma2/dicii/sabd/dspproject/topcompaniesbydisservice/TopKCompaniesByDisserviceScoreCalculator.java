package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.EVENT_TIME_FORMAT;

public class TopKCompaniesByDisserviceScoreCalculator extends ProcessAllWindowFunction<WindowedCompanyDisserviceScore, String, TimeWindow> {

    private int k;

    public TopKCompaniesByDisserviceScoreCalculator(int k) {
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
        // TODO possibly refactor
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
        String printableStartTimestamp = sdf.format(date);
        /* Build the output ranking string */
        StringBuilder output = new StringBuilder(printableStartTimestamp + "");
        for (WindowedCompanyDisserviceScore companyDisserviceScore : topKCompanies) {
            output.append(",").append(companyDisserviceScore.getCompany()).append(",").append(companyDisserviceScore.getDisserviceScore());
        }
        collector.collect(output.toString());
    }
}
