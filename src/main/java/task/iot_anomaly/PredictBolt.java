package task.iot_anomaly;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tribuo.DataSource;
import org.tribuo.Example;
import org.tribuo.MutableDataset;
import org.tribuo.Prediction;
import org.tribuo.anomaly.AnomalyFactory;
import org.tribuo.anomaly.Event;
import org.tribuo.anomaly.libsvm.LibSVMAnomalyTrainer;
import org.tribuo.anomaly.libsvm.SVMAnomalyType;
import org.tribuo.common.libsvm.KernelType;
import org.tribuo.common.libsvm.LibSVMModel;
import org.tribuo.common.libsvm.LibSVMTrainer;
import org.tribuo.common.libsvm.SVMParameters;
import org.tribuo.data.csv.CSVIterator;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.evaluation.TrainTestSplitter;
import org.tribuo.impl.ArrayExample;
import org.tribuo.regression.Regressor;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PredictBolt extends BaseBasicBolt {
    private static final String[] headers = new String[]{"R1","R2","R3","R4","R5","R6","R7","R8","R9","R10",
            "R11","R12","R13","R14","R15","R16"};
    private static LibSVMModel<Event> MODEL = buildModel();

    private static LibSVMModel<Event> buildModel() {
        AnomalyFactory anomalyFactory = new AnomalyFactory();
        CSVLoader<Event> csvLoader = new CSVLoader<>(',', CSVIterator.QUOTE, anomalyFactory);
        DataSource<Event> dataSource = null;
        try {
            dataSource = csvLoader.loadDataSource(Paths.get(Config.csvPath), "Y");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TrainTestSplitter<Event> dataSplitter = new TrainTestSplitter<>(dataSource,0.5,1L);
        MutableDataset<Event> trainSet = new MutableDataset<>(dataSplitter.getTrain());
        SVMParameters<Event> params = new SVMParameters<>(new SVMAnomalyType(SVMAnomalyType.SVMMode.ONE_CLASS), KernelType.RBF);
        params.setGamma(1.0);
        params.setNu(0.1);
        LibSVMTrainer<Event> trainer = new LibSVMAnomalyTrainer(params);
        return trainer.train(trainSet);
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
        List<Prediction<Event>> predictions = MODEL.predict(examples);
        List<Event.EventType> predictEventTypes = predictions.stream()
                .map(t -> t.getOutput().getType()).collect(Collectors.toList());
        collector.emit(new Values(start, type, predictEventTypes));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "type", "predictions"));
    }
}
