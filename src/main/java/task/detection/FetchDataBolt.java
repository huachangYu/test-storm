package task.detection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tribuo.MutableDataset;
import org.tribuo.anomaly.Event;
import org.tribuo.anomaly.example.GaussianAnomalyDataSource;

import java.util.ArrayList;

public class FetchDataBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String type = input.getStringByField("type");
        DatasetParam params = (DatasetParam) input.getValueByField("params");
        GaussianAnomalyDataSource source = new GaussianAnomalyDataSource(params.size, params.fractionAnomalous, params.seed);
        MutableDataset<Event> data = new MutableDataset<>(source);
        collector.emit(new Values(params.id, type, params.eventTime, new ArrayList<>(data.getData())));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "type", "eventTime", "data"));
    }
}
