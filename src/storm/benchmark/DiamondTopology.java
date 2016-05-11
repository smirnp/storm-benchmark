package storm.benchmark;

import itmo.escience.dapris.monitoring.common.CommonMongoClient;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import storm.benchmark.GenSpout;
import storm.benchmark.SingleJoinBolt;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class DiamondTopology{
    //LocalTopo -workers=11 -processBolts=4 -spoutExecutors=1 -processBoltExecutors=2 -joinBoltExecutors=1 -tupleSizeBytes=1 -cpu=90 -memory=887
    public static String[] requiredArguments = new String[]{ "workers", "processBolts", "spoutExecutors", "processBoltExecutors", "joinBoltExecutors", "tupleSizeBytes", "cpu", "memory" };


    //storm jar storm-benchmark-0.0.1-SNAPSHOT-standalone.jar storm.benchmark.ThroughputTest demo 1000000 8 8 8 10000
    //LocalTopo -workers=15 -processBolts=2 -spoutExecutors=7 -processBoltExecutors=8 -finalBoltExecutors=8 -tupleSizeBytes=5 -cpu=13 -memory=512
    //LocalTopo -workers=11 -processBolts=4 -spoutExecutors=1 -processBoltExecutors=2 -joinBoltExecutors=1 -finalBoltExecutors=1 -tupleSizeBytes=1 -cpu=90 -memory=887
    public static void main(String[] args) throws Exception {

        String topoName = args[0];
        HashMap<String, Integer> properties = new HashMap<String, Integer>();
        ArrayList<String> notSetArguments = new ArrayList<String>(Arrays.asList(requiredArguments));
        for(String arg : args)
            if(arg.startsWith("-") && arg.contains("=")){
                String[] splitted = arg.split("=");
                properties.put(splitted[0].substring(1), Integer.parseInt(splitted[1]));
                notSetArguments.remove(splitted[0].substring(1));
            }
        if(notSetArguments.size()>0)
            throw new Exception("Arguments "+ String.join(", ", notSetArguments)+" not set");

        int workers = properties.get("workers");
        int spoutExecutors = properties.get("spoutExecutors");
        int processBolts = properties.get("processBolts");

        int tupleSizeBytes = properties.get("tupleSizeBytes");
        int cpu = properties.get("cpu");
        int memory = properties.get("memory");

        //int maxPending = Integer.parseInt(args[5]);

        TopologyBuilder builder = new TopologyBuilder();

        //spout.setMemoryLoad(5*memKoef);  //640
        SpoutDeclarer spoutDeclarer = builder.setSpout("spout", new GenSpout(new Fields("id","size","text"), processBolts, tupleSizeBytes), spoutExecutors);
        spoutDeclarer.setCPULoad(cpu);
        spoutDeclarer.setMemoryLoad(memory);
        //100
        // builder.setBolt("count", new CountBolt(), bolt).shuffleGrouping("spout");
//                .fieldsGrouping("bolt", new Fields("id"));
        String[] processFields = new String[processBolts+1];
        processFields[0]="id";
        for(int i=1; i<=processBolts; i++){
            BoltDeclarer processBoltXDeclarer = builder.setBolt("processBolt_"+i, new DummyProcessingBolt(new Fields("id", "p"+i), tupleSizeBytes), properties.get("processBoltExecutors")).fieldsGrouping("spout", new Fields("size"));
            processBoltXDeclarer.setCPULoad(cpu);
            processBoltXDeclarer.setMemoryLoad(memory);
            processFields[i]="p"+i;
        }

        BoltDeclarer joinBoltDeclarer = builder.setBolt("joinBolt", new SingleJoinBolt(new Fields(processFields), tupleSizeBytes), properties.get("joinBoltExecutors"));//.fieldsGrouping("processBoltX", new Fields("id")).fieldsGrouping("processBoltY", new Fields("id"));
        for(int i=1; i<=processBolts; i++)
           joinBoltDeclarer = joinBoltDeclarer.fieldsGrouping("processBolt_"+i, new Fields("id"));

        joinBoltDeclarer.setCPULoad(cpu);
        joinBoltDeclarer.setMemoryLoad(memory);

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
