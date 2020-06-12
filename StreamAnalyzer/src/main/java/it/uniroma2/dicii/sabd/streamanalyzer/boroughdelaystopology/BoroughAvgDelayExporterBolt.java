package it.uniroma2.dicii.sabd.streamanalyzer.boroughdelaystopology;

import com.esotericsoftware.minlog.Log;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class BoroughAvgDelayExporterBolt extends AbstractRedisBolt {

    private OutputCollector collector;

    public BoroughAvgDelayExporterBolt(JedisPoolConfig config) {
        super(config);
    }

    public BoroughAvgDelayExporterBolt(JedisClusterConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        this.collector = collector;
    }

    @Override
    protected void process(Tuple tuple) {
        Log.info("dentro process");
        Long startTimestampRaw = tuple.getLongByField("startTimestamp");
        Date date = new Date(startTimestampRaw);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
        String startTimestamp = sdf.format(date);
        HashMap<String, BoroughDelaySummary> boroughAvgDelays = (HashMap<String, BoroughDelaySummary>) tuple.getValueByField("avgDelayByBorough");
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            for (String borough : boroughAvgDelays.keySet()) {
                long aho = jedisCommands.hset(startTimestamp, borough, String.valueOf(boroughAvgDelays.get(borough).getAvg()));
                Log.info("aho=" + aho);
            }
        } catch (Exception e) {
            Log.info("redis stack trace");
            e.printStackTrace();
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
