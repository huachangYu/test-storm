package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.tribuo.anomaly.Event;
import task.common.CommandLine;
import task.iot_anomaly.OutputBolt;
import task.iot_anomaly.ParserBolt;
import task.iot_anomaly.PredictBolt;
import task.iot_anomaly.Source;

import java.util.Arrays;

public class IoTBenchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("args=" + Arrays.toString(args));
        CommandLine.CommandConfig commandConfig = CommandLine.getCLIConfig(args);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source", new Source(commandConfig.qps), 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("predict", new PredictBolt(), 3).fieldsGrouping("parser", new Fields("type"));
        builder.setBolt("output", new OutputBolt(), 1).shuffleGrouping("predict");

        Config conf = new Config();
        conf.registerSerialization(Event.EventType.class);
        if (commandConfig.useThreadPool) {
            conf.useBoltThreadPool(true);
            conf.setBoltThreadPoolCoreThreads(commandConfig.threads);
            conf.setTopologyBoltThreadPoolStrategy(commandConfig.threadPoolStrategy);
            conf.setTopologyBoltThreadPoolFetchMaxTasks(commandConfig.fetchMaxTasks);
            conf.setTopologyBoltThreadPoolIds(Arrays.asList("parser", "predict"));
        }

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("ioTBenchmark", conf, builder.createTopology());
    }
}
