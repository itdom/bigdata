package cn.gxlx.computer.storm.middlebolt;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplitterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(WordSplitterBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    static int count = 0;

    @Override
    public void execute(Tuple input) {
        String record = input.getString(0);
        collector.emit(input, new Values(record, 1));
        collector.ack(input);
        count++;
        LOG.info("boltboltboltboltboltboltboltboltthread=" + Thread.currentThread().getId() + "count=" + count);
        /* if (record != null && !record.trim().isEmpty()) {
             for (String word : record.split("\\s+")) {
                 collector.emit(input, new Values(word, 1));
                 LOG.info("Emitted: word=" + word);
                 collector.ack(input);
             }
         }*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
