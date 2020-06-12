package it.uniroma2.dicii.sabd.streamanalyzer.boroughdelaystopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoroughAvgDelayWindowedBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        Long startTimestamp = tupleWindow.getStartTimestamp();
        List<Tuple> tuplesInWindow = tupleWindow.get();
        HashMap<String, BoroughDelaySummary> boroughDelaysInWindow = new HashMap<>();
        for (Tuple tuple : tuplesInWindow) {
            String borough = tuple.getStringByField("borough");
            Double delay = tuple.getDoubleByField("delay");
            BoroughDelaySummary boroughDelaySummary = boroughDelaysInWindow.get(borough);
            if (boroughDelaySummary == null) {
                boroughDelaySummary = new BoroughDelaySummary();
                boroughDelaysInWindow.put(borough, boroughDelaySummary);
            }
            boroughDelaySummary.setCount(boroughDelaySummary.getCount() + 1);
            boroughDelaySummary.setSum(boroughDelaySummary.getSum() + delay);
        }
        for (String borough : boroughDelaysInWindow.keySet()) {
            BoroughDelaySummary boroughDelaySummary = boroughDelaysInWindow.get(borough);
            boroughDelaySummary.setAvg(boroughDelaySummary.getSum() / boroughDelaySummary.getCount());
        }
        collector.emit(new Values(startTimestamp, boroughDelaysInWindow));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("startTimestamp", "avgDelayByBorough"));
    }
}
