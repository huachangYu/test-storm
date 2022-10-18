package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import task.common.CommandLine;
import task.common.CommonConfig;
import task.common.ConfigUtil;
import task.smoke.ClassficationBolt;
import task.smoke.OutputBolt;
import task.smoke.ParserBolt;
import task.smoke.SmokeSource;

import java.util.Arrays;

public class SmokeBenchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("args=" + Arrays.toString(args));
        CommandLine.CommandConfig commandConfig = CommandLine.getCLIConfig(args);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source", new SmokeSource(commandConfig.qps), 1);
        builder.setBolt("parser", new ParserBolt(), 1).shuffleGrouping("source");
        builder.setBolt("svm", new ClassficationBolt("svm"), 1).shuffleGrouping("parser");
        builder.setBolt("logistic", new ClassficationBolt("logistic"), 1).shuffleGrouping("parser");
        builder.setBolt("cart", new ClassficationBolt("cart"), 1).shuffleGrouping("parser");
        builder.setBolt("output", new OutputBolt(), 1)
                .shuffleGrouping("svm")
                .shuffleGrouping("logistic")
                .shuffleGrouping("cart");
        Config conf = new Config();
        ConfigUtil.updateConfig(conf, commandConfig, Arrays.asList("parser", "svm", "logistic", "cart", "output"));

        if (CommonConfig.isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("smokeBenchmark", conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
