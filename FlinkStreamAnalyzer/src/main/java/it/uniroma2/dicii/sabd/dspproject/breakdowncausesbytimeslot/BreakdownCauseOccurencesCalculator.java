package it.uniroma2.dicii.sabd.dspproject.breakdowncausesbytimeslot;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class BreakdownCauseOccurencesCalculator implements ReduceFunction<Tuple2<Tuple2<String, String>, Long>> {

    @Override
    public Tuple2<Tuple2<String, String>, Long> reduce(Tuple2<Tuple2<String, String>, Long> t1, Tuple2<Tuple2<String, String>, Long> t2)  {
        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
    }
}
