package cn.gxlx.computer.storm.cast;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WriterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -6586283337287975719L;

    private FileWriter writer = null;

    public WriterBolt() {
        System.out.print("&&&&&&&&&&&&&&&&& WriterBolt constructor method invoked");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.print("################# WriterBolt prepare() method invoked");
        try {
            writer = new FileWriter("/home/master/" + this);
        } catch (IOException e) {
            System.out.print(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.print("################# WriterBolt declareOutputFields() method invoked");
    }

    int count = 0;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String s = input.getString(0);
        try {
            count++;
            writer.write(s);
            writer.write("\n");
            writer.flush();
            System.out.println(count + "=" + s + ",thread" + Thread.currentThread().getId() + ",this=" + this);
        } catch (IOException e) {
            System.out.print(e);
            throw new RuntimeException(e);
        }
    }

}
