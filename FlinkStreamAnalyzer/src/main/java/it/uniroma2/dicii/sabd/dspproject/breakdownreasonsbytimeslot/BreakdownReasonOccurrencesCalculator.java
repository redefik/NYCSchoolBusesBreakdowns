package it.uniroma2.dicii.sabd.dspproject.breakdownreasonsbytimeslot;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/*
* Sum the values of the occurrence field in tuples with the same key (time slot and reason)
* */
public class BreakdownReasonOccurrencesCalculator implements ReduceFunction<Tuple2<Tuple2<String, String>, Long>> {

    @Override
    public Tuple2<Tuple2<String, String>, Long> reduce(Tuple2<Tuple2<String, String>, Long> t1, Tuple2<Tuple2<String, String>, Long> t2)  {
        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
    }
}
