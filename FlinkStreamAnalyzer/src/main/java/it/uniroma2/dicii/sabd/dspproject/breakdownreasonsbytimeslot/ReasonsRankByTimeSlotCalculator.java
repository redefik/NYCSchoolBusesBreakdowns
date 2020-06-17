package it.uniroma2.dicii.sabd.dspproject.breakdownreasonsbytimeslot;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.*;

/*
* This class build the ranking of the most frequent reasons for each time slot inside a time window
* */
public class ReasonsRankByTimeSlotCalculator extends ProcessAllWindowFunction<Tuple2<Tuple2<String,String>, Long>, String, TimeWindow> {

    private static final Integer RANK_LENGTH = 3;

    @Override
    public void process(Context context, Iterable<Tuple2<Tuple2<String, String>, Long>> iterable, Collector<String> collector) {

        /* Ordering tuples belonging to each time slot by occurrences */
        ArrayList<Tuple2<Tuple2<String, String>, Long>> morningTimeSlotTuples = new ArrayList<>();
        ArrayList<Tuple2<Tuple2<String, String>, Long>> afternoonTimeSlotTuples = new ArrayList<>();

        for (Tuple2<Tuple2<String, String>, Long> tuple : iterable){
            if (tuple.f0.f0.equals(MORNING_TIME_SLOT_START + "-" + MORNING_TIME_SLOT_END)){
                morningTimeSlotTuples.add(tuple);
            } else if (tuple.f0.f0.equals(AFTERNOON_TIME_SLOT_START + "-" + AFTERNOON_TIME_SLOT_END)){
                afternoonTimeSlotTuples.add(tuple);
            }
        }

        morningTimeSlotTuples.sort(Comparator.comparing(t -> t.f1));
        afternoonTimeSlotTuples.sort(Comparator.comparing(t -> t.f1));

        /* Preparing output string*/
        long startTimestamp = context.window().getStart();
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
        String printableStartTimestamp = sdf.format(date);
        StringBuilder output = new StringBuilder(printableStartTimestamp + ", ");
        int rankLength;

        output.append(MORNING_TIME_SLOT_START + "-" + MORNING_TIME_SLOT_END);
        rankLength = (morningTimeSlotTuples.size() >= RANK_LENGTH) ? (RANK_LENGTH) : morningTimeSlotTuples.size();
        for (int i = 0; i < rankLength; i++){
            output.append(", ").append(morningTimeSlotTuples.get(morningTimeSlotTuples.size() - 1 - i).f0.f1);
        }

        output.append(", " + AFTERNOON_TIME_SLOT_START + "-" + AFTERNOON_TIME_SLOT_END);
        rankLength = (afternoonTimeSlotTuples.size() >= RANK_LENGTH) ? (RANK_LENGTH) : afternoonTimeSlotTuples.size();
        for (int i = 0; i < rankLength; i++){
            output.append(", ").append(afternoonTimeSlotTuples.get(afternoonTimeSlotTuples.size() - 1 - i).f0.f1);
        }

        collector.collect(output.toString());
    }
}
