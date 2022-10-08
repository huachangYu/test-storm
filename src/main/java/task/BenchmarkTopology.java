package task;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.executor.bolt.BoltWeightCalc;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.tribuo.CategoricalInfo;
import org.tribuo.MutableDataset;
import org.tribuo.MutableFeatureMap;
import org.tribuo.Prediction;
import org.tribuo.RealInfo;
import org.tribuo.anomaly.Event;
import org.tribuo.impl.ArrayExample;
import task.bolt.AnomalyDetectBolt;
import task.bolt.FetchDataBolt;
import task.bolt.ParserBolt;
import task.model.DatasetParam;
import task.sink.OutputBolt;
import task.spout.Source;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Predicate;

public class BenchmarkTopology {
    public static void main(String[] args) throws Exception {
        System.out.println("args=" + Arrays.toString(args));
        Options JDUL = new Options();
        JDUL.addOption("useThreadPool"   ,false, "useThreadPool");
        JDUL.addOption("threads" ,true,  "threads");
        JDUL.addOption("threadPoolStrategy", true, "threadPoolStrategy");
        JDUL.addOption("qps", true, "qps");
        JDUL.addOption("fetchMaxTasks", true, "fetchMaxTasks");
        JDUL.addOption("shuffle", false, "shuffle");

        DefaultParser parser = new DefaultParser();
        CommandLine cli = parser.parse(JDUL, args);
        boolean useThreadPool = cli.hasOption("useThreadPool");
        int threads = cli.hasOption("threads") ? Integer.parseInt(cli.getOptionValue("threads")) : 4;
        String threadPoolStrategy = cli.hasOption("threadPoolStrategy") ? cli.getOptionValue("threadPoolStrategy") : BoltWeightCalc.Strategy.Fair.name();
        int fetchMaxTasks = cli.hasOption("fetchMaxTasks") ? Integer.parseInt(cli.getOptionValue("fetchMaxTasks")) : 1;
        int qps = cli.hasOption("qps") ? Integer.parseInt(cli.getOptionValue("qps")) : 1000;
        boolean isShuffle = cli.hasOption("shuffle");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source", new Source(qps), 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("fetch", new FetchDataBolt(), 2).shuffleGrouping("parser");

        if (isShuffle) {
            builder.setBolt("detect", new AnomalyDetectBolt(), 2).shuffleGrouping("fetch");
        } else {
            builder.setBolt("detect", new AnomalyDetectBolt(), 2).fieldsGrouping("fetch", new Fields("type"));
        }
        builder.setBolt("output", new OutputBolt(), 1).shuffleGrouping("detect");

        Config conf = new Config();
        conf.setDebug(false);
        if (useThreadPool) {
            conf.useBoltThreadPool(true);
            conf.setBoltThreadPoolCoreThreads(threads);
            conf.setTopologyBoltThreadPoolStrategy(threadPoolStrategy);
            conf.setTopologyBoltThreadPoolFetchMaxTasks(fetchMaxTasks);
            conf.setTopologyBoltThreadPoolIds(Arrays.asList("parser", "fetch", "detect"));
        }

//        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("benchmark", conf, builder.createTopology());
    }
}
