package storm.benchmark;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by Pavel Smirnov
 */
public class GenSpout extends BaseRichSpout {
    private static final Character[] CHARS = new Character[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
    private int tuplesCounter = 0;
    SpoutOutputCollector _collector;
    int _size;
    int _koef = 1;
    int _processBolts = 0;
    Random _rand;
    int _id;
    String _val;
    private Fields _outFields;

    public GenSpout(Fields outFields, int processBolts, int size) {
        _size = size;
        _outFields = outFields;
        _processBolts = processBolts;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        tuplesCounter++;

        int tupleSize = _koef * 5 * _size;

        _val = randString(tupleSize);
        _collector.emit(new Values(tuplesCounter, tupleSize, _val));
        _koef++;
        if(_koef >_processBolts)
            _koef=1;
    }

    private String randString(int size) {
        StringBuffer buf = new StringBuffer();
        for(int i=0; i<size; i++) {
            buf.append(CHARS[_rand.nextInt(CHARS.length)]);
        }
        return buf.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }
}
