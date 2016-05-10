package storm.benchmark;

import itmo.escience.dapris.monitoring.common.CommonMongoClient;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;

/**
 * Created by Pavel Smirnov
 */
public class DummyProcessingBolt extends BaseBasicBolt {
    private static final Character[] CHARS = new Character[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
    private Fields _outFields;
    int _size;
    int _executions = 0;
    Random _rand;

    public DummyProcessingBolt(Fields outFields, int size){
        this._outFields = outFields;
        this._size = size;
    }
    @Override
    public void prepare(Map conf, TopologyContext context){
        _rand = new Random();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        int koef = 1;
        if(_outFields.contains("x"))
            koef = 7;
        else
            koef = 5;

        for(int i=0; i<koef*2000; i++)
            Math.sin(i);

        int tupleSize = koef * _size;
        String _val = randString(tupleSize);
        collector.emit( new Values(tuple.getIntegerByField("id"), _val));
        _executions++;
    }

    private String randString(int size) {
        StringBuffer buf = new StringBuffer();
        for(int i=0; i<size; i++) {
            buf.append(CHARS[_rand.nextInt(CHARS.length)]);
        }
        return buf.toString();
    }
}