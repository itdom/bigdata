package cn.gxlx.computer.storm.complexbolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer implements IRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
    }

    public void execute(Tuple input) {
        // TODO Auto-generated method stub
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                List a = new ArrayList();
                a.add(input);
                collector.emit(a, new Values(word));
            }
        }
        collector.ack(input);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
