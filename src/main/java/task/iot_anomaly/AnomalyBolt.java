package task.iot_anomaly;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tribuo.Example;
import org.tribuo.Model;
import org.tribuo.Prediction;
import org.tribuo.anomaly.AnomalyFactory;
import org.tribuo.anomaly.Event;
import org.tribuo.impl.ArrayExample;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AnomalyBolt extends BaseBasicBolt {
    private static final String[] headers = new String[]{"R1","R2","R3","R4","R5","R6","R7","R8","R9","R10",
            "R11","R12","R13","R14","R15","R16"};

    private final String modelType; //"svm" or "linear"
    private final Model<Event> model;

    public AnomalyBolt(String modelType) {
        this.modelType = modelType;
        if (modelType.equals("svm")) {
            model = AnomalyModelUtils.getSVMModel();
        } else if (modelType.equals("linear")) {
            model = AnomalyModelUtils.getLinearModel();
        } else {
            throw new IllegalArgumentException("wrong modelType");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long start = input.getLongByField("start");
        String type = input.getStringByField("type");
        List<List<Double>> data = (List<List<Double>>) input.getValueByField("data");
        List<Example<Event>> examples = new ArrayList<>();
        Event outputPlaceHolder = AnomalyFactory.UNKNOWN_EVENT;
        for (List<Double> cells : data) {
            double[] values = cells.stream().mapToDouble(t -> t).toArray();
            Example<Event> example = new ArrayExample<>(outputPlaceHolder, headers, values);
            examples.add(example);
        }
        List<Prediction<Event>> predictions = model.predict(examples);
        List<Event.EventType> predictEventTypes = predictions.stream()
                .map(t -> t.getOutput().getType()).collect(Collectors.toList());
        collector.emit(new Values(start, modelType, type, predictEventTypes));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "modelType", "type", "predictions"));
    }
}
