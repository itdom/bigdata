package cn.gxlx.computer.storm.simplebolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 接收喷发节点(Spout)发送的数据进行简单的处理后，发射出去。
 * 
 * @author Administrator
 * 
 */
public class SimpleBolt extends BaseBasicBolt {

    /**
     * 
     */
    private static final long serialVersionUID = -1901957490777870838L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String msg = input.getString(0);
            if (msg != null) {
                //System.out.println("msg="+msg);
                collector.emit(new Values(msg + "msg is processed!"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("info"));
    }

}
