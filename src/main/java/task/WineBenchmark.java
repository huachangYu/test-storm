package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import task.common.CommandLine;
import task.common.CommonConfig;
import task.common.ConfigUtil;
import task.wine.OutputBolt;
import task.wine.ParserBolt;
import task.wine.AverageBolt;
import task.wine.RegressionBolt;
import task.wine.WineSource;

import java.util.Arrays;

public class WineBenchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("task=WineBenchmark, args=" + Arrays.toString(args));
        CommandLine.CommandConfig commandConfig = CommandLine.getCLIConfig(args);

        TopologyBuilder builder = new TopologyBuilder();
        WineSource source = commandConfig.startQpsUpdater ?
                new WineSource(commandConfig.minQps, commandConfig.maxQps,
                        commandConfig.increaseQps, commandConfig.timeDeltaQps) :
                new WineSource(commandConfig.qps);
        builder.setSpout("source", source, 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("randomForest", new RegressionBolt("randomForest"), 1).shuffleGrouping("parser");
        builder.setBolt("avg", new AverageBolt(), 1).shuffleGrouping("parser");
        builder.setBolt("elasticNet", new RegressionBolt("elasticNet"), 1).shuffleGrouping("avg");
        builder.setBolt("output", new OutputBolt(), 1)
                .shuffleGrouping("randomForest")
                .shuffleGrouping("elasticNet");
        Config conf = new Config();
        ConfigUtil.updateConfig(conf, commandConfig, Arrays.asList("parser", "randomForest", "avg", "elasticNet"));

        if (CommonConfig.isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wineBenchmark", conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
