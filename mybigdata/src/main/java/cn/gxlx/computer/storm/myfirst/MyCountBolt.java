package cn.gxlx.computer.storm.myfirst;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MyCountBolt extends BaseRichBolt {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private BufferedWriter bufferedWriter2;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declarer.declare(new Fields("a", "b", "c", "d"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            bufferedWriter2 = new BufferedWriter(new FileWriter(new File("d://test//b//" + this)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        try {

            String word2 = input.getStringByField("word1");
            bufferedWriter2.write(word2);
            bufferedWriter2.write("\n");

            bufferedWriter2.flush();
            collector.ack(input);
        } catch (IOException e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }
}
