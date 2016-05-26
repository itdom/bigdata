package cn.gxlx.computer.storm.myfirst;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * @author GDL
 *
 */
public class MyTopology {
    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MySpout(), 1);
        topologyBuilder.setBolt("myBolt", new MyCountBolt(), 6).fieldsGrouping("mySpout", new Fields("word2"));

        Map<String, Integer> conf = new HashMap<String, Integer>();
        conf.put(Config.TOPOLOGY_WORKERS, 4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, topologyBuilder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
