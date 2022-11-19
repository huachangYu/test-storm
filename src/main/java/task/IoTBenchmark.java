package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.tribuo.anomaly.Event;
import task.common.CommandLine;
import task.common.CommonConfig;
import task.common.ConfigUtil;
import task.iot_anomaly.AnomalyBolt;
import task.iot_anomaly.IoTOutputBolt;
import task.iot_anomaly.ParserBolt;
import task.iot_anomaly.IoTSource;

import java.util.Arrays;

public class IoTBenchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("task=IoTBenchmark, args=" + Arrays.toString(args));
        CommandLine.CommandConfig commandConfig = CommandLine.getCLIConfig(args);

        TopologyBuilder builder = new TopologyBuilder();
        IoTSource source = commandConfig.startQpsUpdater ?
                new IoTSource(commandConfig.minQps, commandConfig.maxQps,
                        commandConfig.increaseQps, commandConfig.timeDeltaQps) :
                new IoTSource(commandConfig.qps);
        builder.setSpout("source", source, 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("svm", new AnomalyBolt("svm"), 2).fieldsGrouping("parser", new Fields("type"));
        builder.setBolt("output", new IoTOutputBolt(), 1).shuffleGrouping("svm");

        Config conf = new Config();
        conf.registerSerialization(Event.EventType.class);
        ConfigUtil.updateConfig(conf, commandConfig, Arrays.asList("parser", "svm"));

        if (CommonConfig.isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("iotBenchmark", conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
