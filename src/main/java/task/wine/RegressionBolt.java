package task.wine;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tribuo.Example;
import org.tribuo.Model;
import org.tribuo.Prediction;
import org.tribuo.impl.ArrayExample;
import org.tribuo.regression.RegressionFactory;
import org.tribuo.regression.Regressor;

import java.util.ArrayList;
import java.util.List;

public class RegressionBolt extends BaseBasicBolt {
    private final String modelType;
    private final Model<Regressor> model;

    public RegressionBolt(String modelType) {
        this.modelType = modelType;
        switch (modelType) {
            case "sgd":
                model = RegressionModelUtils.getLinearSGDModel();
                break;
            case "fm":
                model = RegressionModelUtils.getFMModel();
                break;
            case "cart":
                model = RegressionModelUtils.getCARTRegressionModel();
                break;
            case "elasticNet":
                model = RegressionModelUtils.getElasticNetModel();
                break;
            case "randomForest":
                model = RegressionModelUtils.getRandomForestModel();
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
        List<Example<Regressor>> examples = new ArrayList<>();
        Regressor outputPlaceHolder = RegressionFactory.UNKNOWN_REGRESSOR;
        for (List<Double> cells : data) {
            double[] values = cells.stream().mapToDouble(t -> t).toArray();
            Example<Regressor> example = new ArrayExample<>(outputPlaceHolder, RegressionModelUtils.dataHeaders, values);
            examples.add(example);
        }
        List<Prediction<Regressor>> predictions = model.predict(examples);
        double prediction = predictions.stream()
                .mapToDouble(t -> Math.min(10, Math.max(0, t.getOutput().getValues()[0])))
                .average()
                .getAsDouble();
        collector.emit(new Values(start, modelType, type, prediction));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "modelType", "type", "prediction"));
    }
}
