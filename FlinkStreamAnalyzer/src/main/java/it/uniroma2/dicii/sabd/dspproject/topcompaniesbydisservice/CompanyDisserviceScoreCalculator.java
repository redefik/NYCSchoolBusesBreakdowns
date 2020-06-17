package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/*
* This class computes the disservice score earned by a school bus company during a time window
* The score is obtained in the following way:
* score = w_t * t + w_m * m + w_o * o
* where w_t + w_m + w_o = 1
* t = delays due to Heavy Traffic
* m = delays due to Mechanical Problem
* o = delays due to Other Reason
* Furthermore, if the delay length is > s minutes, then the event counts twice
*
* Here the score is computed incrementally maintaining an accumulator consisting of three values: count of heavy traffic
* delays; count of mechanical problem delays and count of other reason delays
* */
public class CompanyDisserviceScoreCalculator implements AggregateFunction<Tuple3<Double, String, String>, Tuple3<Long, Long, Long>, Double> {

    private static final String HEAVY_TRAFFIC = "Heavy Traffic";
    private static final double HEAVY_TRAFFIC_WEIGHT = 0.3;
    private static final String MECHANICAL_PROBLEM = "Mechanical Problem";
    private static final double MECHANICAL_PROBLEM_WEIGHT = 0.5;
    private static final double OTHER_REASON_WEIGHT = 0.2;
    private static final double BIG_DELAY_THRESHOLD = 30.0;

    @Override
    public Tuple3<Long, Long, Long> createAccumulator() {
        return new Tuple3<>(0L, 0L, 0L);
    }

    @Override
    public Tuple3<Long, Long, Long> add(Tuple3<Double, String, String> newTuple, Tuple3<Long, Long, Long> oldAccumulator) {
        Long increment = 1L;
        Double delay = newTuple.f0;
        String reason = newTuple.f1;
        /* Check if the event counts twice */
        if (delay > BIG_DELAY_THRESHOLD) {
            increment = increment * 2;
        }
        /* Update the accumulator according to the delay reason */
        if (reason.equals(HEAVY_TRAFFIC)) {
            return new Tuple3<>(oldAccumulator.f0 + increment, oldAccumulator.f1, oldAccumulator.f2);
        } else if (reason.equals(MECHANICAL_PROBLEM)) {
            return new Tuple3<>(oldAccumulator.f0, oldAccumulator.f1 + increment, oldAccumulator.f2);
        } else {
            return new Tuple3<>(oldAccumulator.f0, oldAccumulator.f1, oldAccumulator.f2 + increment);
        }
    }

    @Override
    public Double getResult(Tuple3<Long, Long, Long> finalCount) {
        return HEAVY_TRAFFIC_WEIGHT * finalCount.f0 + MECHANICAL_PROBLEM_WEIGHT * finalCount.f1 + OTHER_REASON_WEIGHT * finalCount.f2;
    }

    @Override
    public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> acc0, Tuple3<Long, Long, Long> acc1) {
        return new Tuple3<>(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1, acc0.f2 + acc1.f2);
    }
}
