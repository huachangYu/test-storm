package task.detection;

import com.google.gson.Gson;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ParserBolt extends BaseBasicBolt {
    private static final Gson GSON = new Gson();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String dataStr = input.getStringByField("data");
        DatasetParam params = GSON.fromJson(dataStr, DatasetParam.class);
        collector.emit(new Values(params.type, params));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "params"));
    }
}