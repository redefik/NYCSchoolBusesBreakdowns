package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

/*
* This class represents the average bus delay registered in a time window for a specific county
* */
public class WindowedCountyAvgDelay {

    private String county;
    private Double avgDelay;

    public WindowedCountyAvgDelay() { }

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
}
