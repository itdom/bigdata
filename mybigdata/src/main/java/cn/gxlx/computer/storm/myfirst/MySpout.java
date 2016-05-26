package cn.gxlx.computer.storm.myfirst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout extends BaseRichSpout {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {

        BufferedReader bre = null;
        try {
            bre = new BufferedReader(new InputStreamReader(new FileInputStream(new File("d://test/test.txt"))));
            String line = null;
            while ((line = bre.readLine()) != null) {
                List<Object> values = new Values(line);
                collector.emit(new Values(line + 1, line.hashCode() % 3 + ""), values);
            }
            bre.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bre != null) {
                try {
                    bre.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word1", "word2"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.print("ack-msgId=" + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.print("fail-msgId=" + msgId);
    }
}
