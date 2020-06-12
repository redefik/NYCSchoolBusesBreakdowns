package it.uniroma2.dicii.sabd.streamanalyzer.boroughdelaystopology;

import it.uniroma2.dicii.sabd.streamanalyzer.ParsingDebugKafkaMapper;
import it.uniroma2.dicii.sabd.streamanalyzer.WindowDebugKafkaMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BoroughDelaysTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        if (args.length != 4) {
            System.err.println("Required: <kafka address> <kafka source topic> <redis_host> <redis_port>");
            System.exit(1);
        }


        TopologyBuilder builder = new TopologyBuilder();

        // TODO ? external config
        KafkaSpoutConfig<String, String> kafkaSpoutConfig =
                KafkaSpoutConfig.builder(args[0],args[1])
                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                        .build();

        builder.setSpout("breakdownsConsumer", new KafkaSpout<>(kafkaSpoutConfig));

        builder.setBolt("boroughDelayExtractor", new BoroughDelayExtractorBolt()).shuffleGrouping("breakdownsConsumer");

        // TODO DEBUG
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.2.15:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "storm-kafka-producer");


        KafkaBolt<String, String> firstKafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("parsingdebug"))
                .withTupleToKafkaMapper(new ParsingDebugKafkaMapper());

        builder.setBolt("forwardToKafka", firstKafkaBolt).globalGrouping("boroughDelayExtractor");

        builder.setBolt("boroughAvgDelayWindowed", new BoroughAvgDelayWindowedBolt()
                .withWindow(new BaseWindowedBolt.Duration(24, TimeUnit.HOURS),new BaseWindowedBolt.Duration(1, TimeUnit.HOURS))
                .withTimestampField("ts")
                .withLag(new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS)),2
        ).fieldsGrouping("boroughDelayExtractor", new Fields("borough"));

        KafkaBolt<String, String> secondKafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("windowdebug"))
                .withTupleToKafkaMapper(new WindowDebugKafkaMapper());

        builder.setBolt("forwardToKafka2", secondKafkaBolt).globalGrouping("boroughAvgDelayWindowed");

        JedisPoolConfig redisConfig = new JedisPoolConfig.Builder()
                .setHost(args[2])
                .setPort(Integer.parseInt(args[3]))
                .build();

        builder.setBolt("boroughAvgDelayExporter", new BoroughAvgDelayExporterBolt(redisConfig)).globalGrouping("boroughAvgDelayWindowed");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);
        config.setMessageTimeoutSecs(100000);
        config.registerSerialization(BoroughDelaySummary.class);
        StormSubmitter.submitTopology("boroughDelaysTopology", config, builder.createTopology());


    }
}
