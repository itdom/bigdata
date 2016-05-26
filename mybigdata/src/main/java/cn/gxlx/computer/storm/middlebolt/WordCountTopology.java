package cn.gxlx.computer.storm.middlebolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
            InterruptedException, AuthorizationException {
        // configure & build topology
        TopologyBuilder builder = new TopologyBuilder();

        String[] records = new String[] { "ni hao", "lv ran", "huan huan" };

        builder.setSpout("spout-producer", new ProduceRecordSpout(records), 3).setNumTasks(3);
        /*    builder.setBolt("bolt-splitter", new WordSplitterBolt(), 3).shuffleGrouping("spout-producer").setNumTasks(3);
            builder.setBolt("bolt-counter", new WordCounterBolt(), 1).fieldsGrouping("bolt-splitter", new Fields("word"))
                    .setNumTasks(2);*/

        // submit topology
        Config conf = new Config();
        String name = WordCountTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_HOST, nimbus);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            System.out.print("mainThread=" + Thread.currentThread().getId());
            cluster.submitTopology(name, conf, builder.createTopology());

            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
}
