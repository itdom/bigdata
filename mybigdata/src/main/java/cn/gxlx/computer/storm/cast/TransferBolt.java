package cn.gxlx.computer.storm.cast;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TransferBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 4223708336037089125L;

    public TransferBolt() {
        System.out.print("&&&&&&&&&&&&&&&&& TransferBolt constructor method invoked");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.print("################# TransferBolt prepare() method invoked");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.print("################# TransferBolt declareOutputFields() method invoked");
        declarer.declare(new Fields("word1", "word2"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.print("################# TransferBolt execute() method invoked");
        String word = input.getStringByField("str");
        collector.emit(new Values(word, word.hashCode() % 4 + ""));
    }

}
