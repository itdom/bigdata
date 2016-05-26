package cn.gxlx.computer.storm.cast;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = -4287209449750623371L;

    private SpoutOutputCollector collector;

    private String[] words = new String[] { "a1", "b22", "c333", "d4444" };

    private Random random = new Random();

    public RandomWordSpout() {
        System.out.print("&&&&&&&&&&&&&&&&& RandomWordSpout constructor method invoked");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.out.print("################# RandomWordSpout open() method invoked");
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.print("################# RandomWordSpout declareOutputFields() method invoked");
        declarer.declare(new Fields("str"));
    }

    @Override
    public void nextTuple() {
        System.out.print("################# RandomWordSpout nextTuple() method invoked");
        Utils.sleep(500);
        String str = words[random.nextInt(words.length)];
        collector.emit(new Values(str));
    }

    @Override
    public void activate() {
        System.out.print("################# RandomWordSpout activate() method invoked");
    }

    @Override
    public void deactivate() {
        System.out.print("################# RandomWordSpout deactivate() method invoked");
    }

}
