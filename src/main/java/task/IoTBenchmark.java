package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.tribuo.anomaly.Event;
import task.common.CommandLine;
import task.common.CommonConfig;
import task.common.ConfigUtil;
import task.iot_anomaly.AnomalyBolt;
import task.iot_anomaly.OutputBolt;
import task.iot_anomaly.ParserBolt;
import task.iot_anomaly.IoTSource;

import java.util.Arrays;

public class IoTBenchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("args=" + Arrays.toString(args));
        CommandLine.CommandConfig commandConfig = CommandLine.getCLIConfig(args);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source", new IoTSource(commandConfig.qps), 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("svm", new AnomalyBolt("svm"), 2).shuffleGrouping("parser");
        builder.setBolt("output", new OutputBolt(), 1).shuffleGrouping("svm");

        Config conf = new Config();
        conf.registerSerialization(Event.EventType.class);
        ConfigUtil.updateConfig(conf, commandConfig, Arrays.asList("parser", "svm", "output"));

        if (CommonConfig.isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("iotBenchmark", conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
