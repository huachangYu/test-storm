package task.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tribuo.MutableDataset;
import org.tribuo.Prediction;
import org.tribuo.anomaly.Event;
import org.tribuo.anomaly.example.GaussianAnomalyDataSource;
import org.tribuo.anomaly.libsvm.LibSVMAnomalyTrainer;
import org.tribuo.anomaly.libsvm.SVMAnomalyType;
import org.tribuo.common.libsvm.KernelType;
import org.tribuo.common.libsvm.LibSVMModel;
import org.tribuo.common.libsvm.LibSVMTrainer;
import org.tribuo.common.libsvm.SVMParameters;
import task.model.DatasetParam;

import java.util.List;

public class AnomalyDetectBolt extends BaseBasicBolt {
    private static LibSVMModel<Event> model;

    static {
        MutableDataset<Event> trainDataSet = new MutableDataset<>(new GaussianAnomalyDataSource(2000, 0.0f, 1L));
        SVMParameters<Event> params = new SVMParameters<>(new SVMAnomalyType(SVMAnomalyType.SVMMode.ONE_CLASS), KernelType.RBF);
        params.setGamma(1.0);
        params.setNu(0.1);
        LibSVMTrainer<Event> trainer = new LibSVMAnomalyTrainer(params);
        model = trainer.train(trainDataSet);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long id = (Long) input.getValueByField("id");
        String type = (String) input.getValueByField("type");
        Long eventTime = (Long) input.getValueByField("eventTime");
        MutableDataset<Event> testData = (MutableDataset<Event>) input.getValueByField("data");
        List<Prediction<Event>> predictions = model.predict(testData);
        collector.emit(new Values(id, type, eventTime, predictions));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "type", "eventTime", "predictions"));
    }
}
