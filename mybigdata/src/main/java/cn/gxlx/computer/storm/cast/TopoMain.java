package cn.gxlx.computer.storm.cast;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopoMain {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        String name = TopoMain.class.getSimpleName();

        if (args != null && args.length > 0) {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("random", new RandomWordSpout(), 2);
            builder.setBolt("transfer", new TransferBolt(), 4).shuffleGrouping("random");
            builder.setBolt("writer", new WriterBolt(), 4).fieldsGrouping("transfer", new Fields("word2"));
            conf.setNumWorkers(2);
            conf.setDebug(true);
            System.out.print("$$$$$$$$$$$ submitting topology...");
            StormSubmitter.submitTopology(name, conf, builder.createTopology());
            System.out.print("$$$$$$$4$$$ topology submitted !");

        } else {

            TopologyBuilder builder = new TopologyBuilder();
            LocalCluster cluster = new LocalCluster();
            System.out.print("mainThread=" + Thread.currentThread().getId());
            builder.setSpout("random", new RandomWordSpout(), 2);
            builder.setBolt("transfer", new TransferBolt(), 4).shuffleGrouping("random");
            builder.setBolt("writer", new WriterBolt(), 1).fieldsGrouping("transfer", new Fields("word1"))
                    .setNumTasks(9);
            cluster.submitTopology(name, conf, builder.createTopology());

            Thread.sleep(60000);
            cluster.shutdown();
        }

    }

}
