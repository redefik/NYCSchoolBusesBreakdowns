package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
 * This class enriches the disservice score earned by a company during a time window with the name of the county.
 * (Used in combination with CompanyDisserviceScoreCalculator)
 * */
public class WindowedCompanyDisserviceScoreCalculator extends ProcessWindowFunction<Double, WindowedCompanyDisserviceScore, String, TimeWindow> {

    @Override
    public void process(String company, Context context, Iterable<Double> iterable, Collector<WindowedCompanyDisserviceScore> collector) {
        Double disserviceScore = iterable.iterator().next();
        WindowedCompanyDisserviceScore windowedCompanyDisserviceScore = new WindowedCompanyDisserviceScore();
        windowedCompanyDisserviceScore.setCompany(company);
        windowedCompanyDisserviceScore.setDisserviceScore(disserviceScore);
        collector.collect(windowedCompanyDisserviceScore);
    }
}
