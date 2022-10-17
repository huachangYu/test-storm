package task.iot_anomaly;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bouncycastle.util.Arrays;
import org.tribuo.Example;
import org.tribuo.Model;
import org.tribuo.Prediction;
import org.tribuo.anomaly.AnomalyFactory;
import org.tribuo.anomaly.Event;
import org.tribuo.classification.Label;
import org.tribuo.classification.LabelFactory;
import org.tribuo.impl.ArrayExample;
import task.smoke.PredictionModelUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PredictionBolt extends BaseBasicBolt {
    private final String modelType; //"svm" or "linear"
    private final Model<Label> model;

    public PredictionBolt(String modelType) {
        this.modelType = modelType;
        switch (modelType) {
            case "svm":
                model = PredictionModelUtils.getSvmModel();
                break;
            case "logistic":
                model = PredictionModelUtils.getLogisticModel();
                break;
            case "cart":
                model = PredictionModelUtils.getCartModel();
                break;
            default:
                throw new IllegalArgumentException("wrong modelType");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long start = input.getLongByField("start");
        String type = input.getStringByField("type");
        List<List<Double>> data = (List<List<Double>>) input.getValueByField("data");
        List<Example<Label>> examples = new ArrayList<>();
        Label outputPlaceHolder = LabelFactory.UNKNOWN_LABEL;
        for (List<Double> cells : data) {
            double[] values = cells.stream().mapToDouble(t -> t).toArray();
            Example<Label> example = new ArrayExample<>(outputPlaceHolder, PredictionModelUtils.dataHeaders, values);
            examples.add(example);
        }
        List<Prediction<Label>> predictions = model.predict(examples);
        List<String> predictLabels = predictions.stream().map(t -> t.getOutput().getLabel()).collect(Collectors.toList());
        collector.emit(new Values(start, modelType, type, predictLabels));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "modelType", "type", "predictions"));
    }
}
