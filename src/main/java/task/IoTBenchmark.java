package task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.tribuo.anomaly.Event;
import task.iot_anomaly.OutputBolt;
import task.iot_anomaly.ParserBolt;
import task.iot_anomaly.PredictBolt;
import task.iot_anomaly.Source;

public class IoTBenchmark {
    public static void main(String[] args) throws Exception {
        int qps = 500;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source", new Source(qps), 1);
        builder.setBolt("parser", new ParserBolt(), 2).shuffleGrouping("source");
        builder.setBolt("predict", new PredictBolt(), 4).fieldsGrouping("parser", new Fields("type"));
        builder.setBolt("output", new OutputBolt(), 1).shuffleGrouping("predict");
        Config conf = new Config();
        conf.registerSerialization(Event.EventType.class);
        conf.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("benchmark", conf, builder.createTopology());
    }
}
