package storm.benchmark;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ThroughputTest {
    public static String mongoHost = "192.168.13.132";
    public static String dbName = "logging";
    public static String statsCollection = "storm.run";

    public static class GenSpout extends BaseRichSpout {
        private static final Character[] CHARS = new Character[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
        private int tuplesCounter = 0;
        SpoutOutputCollector _collector;
        int _size;
        Random _rand;
        String _id;
        String _val;
        //CommonMongoClient commonMongoClient;
        String topoName;
        
        public GenSpout(String topoName, int size) {
            _size = size;
            this.topoName = topoName;
        }
        
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
            _id = randString(5);
            _val = randString(_size);
            //commonMongoClient = new CommonMongoClient(ThroughputTest.mongoHost, null, null, ThroughputTest.dbName);
        }

        @Override
        public void nextTuple() {
            tuplesCounter++;
            //TupleStat tupleStat = new TupleStat(topoName, tuplesCounter, new Date(), _size);
            //commonMongoClient.saveObjectToDB(statsCollection+"."+topoName.split("_")[0], tupleStat);
            _collector.emit(new Values(tuplesCounter, _val));

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
            declarer.declare(new Fields("id", "item"));
        }        
    }
    
    public static class IdentityBolt extends BaseBasicBolt{
        int taskID;
        String hostname;
        String componentID;
        String topoName;

        public IdentityBolt(String topoName){
            this.topoName = topoName;
        }
        @Override
        public void prepare(Map conf, TopologyContext context){
            //commonMongoClient = new CommonMongoClient(ThroughputTest.mongoHost, null, null, ThroughputTest.dbName);
            taskID = context.getThisTaskId();
            componentID = context.getComponentId(taskID);
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e){
                //e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "item"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
//            DBObject condition = new BasicDBObject();
//            condition.put("tupleID", tuple.getValueByField("id"));
//            condition.put("topoName", topoName);
//            List<TupleStat> tupleStats = commonMongoClient.getObjectsFromDB(ThroughputTest.statsCollection+"."+topoName.split("_")[0], condition, 1, TupleStat.class);

//            TupleStat tupleStat = tupleStats.get(0);
//            tupleStat.started = new Date();
//            tupleStat.taskID = this.taskID;
//            tupleStat.componentID = this.componentID;
//            tupleStat.host = this.hostname;

            //commonMongoClient.saveObjectToDB(ThroughputTest.statsCollection+"."+topoName.split("_")[0], condition, tupleStat);
            for(int i=0; i<10000; i++)
                Math.sin(i);
            collector.emit(tuple.getValues());

            //tupleStat.finished = new Date();
            //commonMongoClient.saveObjectToDB(ThroughputTest.statsCollection+"."+topoName.split("_")[0], condition, tupleStat);
        }        
    }

    public static class CountBolt extends BaseBasicBolt {
        int _count;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("count"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            _count+=1;
            collector.emit(new Values(_count));
        }        
    }
    
    public static class AckBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
        }       
    }
    
    
    //storm jar storm-benchmark-0.0.1-SNAPSHOT-standalone.jar storm.benchmark.ThroughputTest demo 1000000 8 8 8 10000
    //LocalTopo 15 7 8 5 13 512
    public static void main(String[] args) throws Exception {
        String topoName = args[0]; //+"_"+new Date().getTime();
        int workers = Integer.parseInt(args[1]);
        int spoutExecutors = Integer.parseInt(args[2]);
        int boltExecutors = Integer.parseInt(args[3]);
        int size = Integer.parseInt(args[4]);

        int cpu = Integer.parseInt(args[5]);
        int memory = Integer.parseInt(args[6]);

        //int maxPending = Integer.parseInt(args[5]);

        TopologyBuilder builder = new TopologyBuilder();

        //spout.setMemoryLoad(5*memKoef);  //640
        SpoutDeclarer spoutDeclarer = builder.setSpout("spout", new GenSpout(topoName,size), spoutExecutors);
        spoutDeclarer.setCPULoad(cpu);
        spoutDeclarer.setMemoryLoad(memory);
        //100
       // builder.setBolt("count", new CountBolt(), bolt).shuffleGrouping("spout");
//                .fieldsGrouping("bolt", new Fields("id"));


        BoltDeclarer boltDeclarer = builder.setBolt("bolt",new IdentityBolt(topoName), boltExecutors).shuffleGrouping("spout");
        boltDeclarer.setCPULoad(cpu);
        boltDeclarer.setMemoryLoad(memory);  //640

//                .shuffleGrouping("spout");
        //builder.setBolt("bolt2", new AckBolt(), bolt).shuffleGrouping("spout");
//        builder.setBolt("count2", new CountBolt(), bolt)
//                .fieldsGrouping("bolt2", new Fields("id"));

        Config conf = new Config();
        conf.setNumWorkers(workers);
        //conf.setMaxSpoutPending(maxPending);
        conf.setNumAckers(0);
        conf.setNumEventLoggers(0);

        //conf.setStatsSampleRate(0.0001);
        //topology.executor.receive.buffer.size: 8192 #batched
        //topology.executor.send.buffer.size: 8192 #individual messages
        //topology.transfer.buffer.size: 1024 # batched
        
//        conf.put("topology.executor.send.buffer.size", 1024);
//        conf.put("topology.transfer.buffer.size", 8);
//        conf.put("topology.receiver.buffer.size", 8);
//        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xdebug -Xrunjdwp:transport=dt_socket,address=1%ID%,server=y,suspend=n");

        if (topoName.startsWith("LocalTopo")) {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology(topoName, conf, topology);
            Utils.sleep(5*60*1000);
        }else{
            StormSubmitter.submitTopology(topoName, conf, builder.createTopology());
        }

    }
}
