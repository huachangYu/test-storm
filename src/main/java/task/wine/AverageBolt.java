package task.wine;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tribuo.MutableDataset;
import org.tribuo.clustering.ClusterID;
import org.tribuo.clustering.example.GaussianClusterDataSource;
import org.tribuo.clustering.kmeans.KMeansTrainer;

import java.util.ArrayList;
import java.util.List;

public class AverageBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long start = input.getLongByField("start");
        String type = input.getStringByField("type");
        List<List<Double>> data = (List<List<Double>>) input.getValueByField("data");
        if (data.size() == 0) {
            return;
        }
        double[] avg = new double[data.get(0).size()];
        for (List<Double> d : data) {
            for (int i = 0; i < d.size(); i++) {
                avg[i] += d.get(i);
            }
        }
        List<Double> t = new ArrayList<>();
        for (int i = 0; i < data.get(0).size(); i++) {
            avg[i] /= data.get(0).size();
            t.add(avg[i]);
        }
        List<List<Double>> outputData = new ArrayList<>();
        outputData.add(t);
        collector.emit(new Values(start, type, data));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "type", "data"));
    }
}
