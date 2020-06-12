package it.uniroma2.dicii.sabd.streamanalyzer.boroughdelaystopology;

public class BoroughDelaySummary {

    private Double sum = 0.0;
    private Double count = 0.0;
    private Double avg = 0.0;

    public BoroughDelaySummary() {
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return  "" + avg;
    }
}
