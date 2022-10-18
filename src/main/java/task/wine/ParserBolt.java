package task.wine;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class ParserBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long start = input.getLongByField("start");
        String type = input.getStringByField("type");
        List<String> lines = (ArrayList<String>)input.getValueByField("data");
        List<List<Double>> data = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split(",");
            List<Double> tData = new ArrayList<>();
            for (String part : parts) {
                tData.add(Double.parseDouble(part.trim()));
            }
            tData.remove(tData.size() - 1);
            data.add(tData);
        }
        collector.emit(new Values(start, type, data));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "type", "data"));
    }
}
