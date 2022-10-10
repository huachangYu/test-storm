package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import task.common.CommandLine;
import task.common.ConfigUtil;
import task.detection.AnomalyDetectBolt;
import task.detection.FetchDataBolt;
import task.detection.OutputBolt;
import task.detection.ParserBolt;
import task.detection.Source;

import java.util.Arrays;

public class BenchmarkTopology {
    public static void main(String[] args) throws Exception {
        System.out.println("args=" + Arrays.toString(args));
        CommandLine.CommandConfig commandConfig = CommandLine.getCLIConfig(args);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source", new Source(commandConfig.qps), 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("fetch", new FetchDataBolt(), 2).shuffleGrouping("parser");
        builder.setBolt("detect", new AnomalyDetectBolt(), 2).fieldsGrouping("fetch", new Fields("type"));
        builder.setBolt("output", new OutputBolt(), 1).shuffleGrouping("detect");

        Config conf = new Config();
        ConfigUtil.updateConfig(conf, commandConfig, Arrays.asList("parser", "fetch", "detect"));

//        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("benchmark", conf, builder.createTopology());
    }
}
