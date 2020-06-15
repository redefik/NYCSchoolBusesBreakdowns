package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.EVENT_TIME_FORMAT;

/* This class represents the average delay registered in a time window for a specific county */
public class WindowedCountyAvgDelay {

    private Long startTimestamp;
    private String county;
    private Double avgDelay;

    public WindowedCountyAvgDelay() {
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public Double getAvgDelay() {
        return avgDelay;
    }

    public void setAvgDelay(Double avgDelay) {
        this.avgDelay = avgDelay;
    }

    // todo REMOVE eventually
    @Override
    public String toString() {
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
        String printableTimestamp = sdf.format(date);
        return "(" + printableTimestamp +
                "," + county +
                "," + avgDelay +
                ')';
    }
}
