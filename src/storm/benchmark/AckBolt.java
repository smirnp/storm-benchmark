package storm.benchmark;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Pavel Smirnov
 */
public class AckBolt extends BaseRichBolt {
    OutputCollector _collector;
    int _count;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple){
        _count+=1;
        _collector.ack(tuple);

    }
}
