package it.uniroma2.dicii.sabd.dspproject.avgdelaysbycounty;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/*
* Computes the average delay in a specific county during a specific time window.
* The computation is made incrementally through an accumulator that tracks the overall delay and the count of delays
* */
public class AvgDelayCalculator implements AggregateFunction<Tuple2<String, Double>, Tuple2<Double, Long>, Double> {
    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Tuple2<String, Double> newTuple, Tuple2<Double, Long> oldAccumulator) {
        return new Tuple2<>(newTuple.f1 + oldAccumulator.f0, oldAccumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return accumulator.f0 / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> acc0, Tuple2<Double, Long> acc1) {
        return new Tuple2<>(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1);
    }
}
