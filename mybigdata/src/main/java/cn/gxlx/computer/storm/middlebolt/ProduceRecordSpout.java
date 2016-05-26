package cn.gxlx.computer.storm.middlebolt;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ProduceRecordSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(ProduceRecordSpout.class);
    private SpoutOutputCollector collector;
    private Random random;
    private String[] records;

    public ProduceRecordSpout(String[] records) {
        this.records = records;
    }

    @Override
    public void activate() {
        LOG.info("==================================================================" + Thread.currentThread().getId());
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        random = new Random();
        LOG.info("--------------------------------------------------------------" + Thread.currentThread().getId());

    }

    static int count = 0;

    @Override
    public void nextTuple() {
        /*   Utils.sleep(5000);
           String record = records[random.nextInt(records.length)];
           List<Object> values = new Values(record);
           //collector.emit(values, values);
           LOG.info("Record emitted: record=" + record + "thread=" + Thread.currentThread().getId());*/
        String record = records[random.nextInt(records.length)];
        List<Object> values = new Values(record);
        count++;
        collector.emit(values, values);
        LOG.info("spoutspoutspoutspoutspoutspoutspout=" + Thread.currentThread().getId() + "count=" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
        LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
                + Thread.currentThread().getId());
    }
}
