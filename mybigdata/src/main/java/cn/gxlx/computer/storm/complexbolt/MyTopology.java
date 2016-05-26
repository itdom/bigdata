package cn.gxlx.computer.storm.complexbolt;

import java.util.Arrays;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class MyTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
            InterruptedException, AuthorizationException {
        BrokerHosts brokerHosts = new ZkHosts("slave01:2181,slave03:2181,slave04:2181");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "kafka_storm", "/mytop3", "word");

        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        spoutConf.zkServers = Arrays.asList(new String[] { "slave01", "slave03", "slave04" });
        spoutConf.zkPort = Integer.valueOf(2181);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new KafkaSpout(spoutConf), 1);
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 1).fieldsGrouping("word-normalizer", new Fields("word"));

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.put(Config.NIMBUS_HOST, "slave07");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology("MyTopology", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("local-host", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
        Utils.sleep(10000);
    }

}
