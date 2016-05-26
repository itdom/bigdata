package cn.gxlx.computer.storm.middlebolt;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.guava.collect.Maps;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCounterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(WordCounterBolt.class);
    private OutputCollector collector;
    private final Map<String, AtomicInteger> counterMap = Maps.newHashMap();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        int count = input.getIntegerByField("count"); // 通过Field名称取出对应字段的数据
        AtomicInteger ai = counterMap.get(word);
        if (ai == null) {
            ai = new AtomicInteger(0);
            counterMap.put(word, ai);
        }
        ai.addAndGet(count);
        LOG.info("DEBUG: word=" + word + ", count=" + ai.get());
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        // print count results
        LOG.info("Word count results:");
        for (Entry<String, AtomicInteger> entry : counterMap.entrySet()) {
            LOG.info("\tword=" + entry.getKey() + ", count=" + entry.getValue().get());
        }
    }

}