package task.iot_anomaly;

import org.tribuo.DataSource;
import org.tribuo.Model;
import org.tribuo.MutableDataset;
import org.tribuo.Trainer;
import org.tribuo.anomaly.AnomalyFactory;
import org.tribuo.anomaly.Event;
import org.tribuo.anomaly.liblinear.LibLinearAnomalyTrainer;
import org.tribuo.anomaly.libsvm.LibSVMAnomalyTrainer;
import org.tribuo.anomaly.libsvm.SVMAnomalyType;
import org.tribuo.common.libsvm.KernelType;
import org.tribuo.common.libsvm.SVMParameters;
import org.tribuo.data.csv.CSVIterator;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.evaluation.TrainTestSplitter;
import task.common.CommonConfig;

import java.io.IOException;
import java.nio.file.Paths;

public class AnomalyModelUtils {
    public static MutableDataset<Event> getTrainingData() {
        AnomalyFactory anomalyFactory = new AnomalyFactory();
        CSVLoader<Event> csvLoader = new CSVLoader<>(',', CSVIterator.QUOTE, anomalyFactory);
        DataSource<Event> dataSource = null;
        try {
            dataSource = csvLoader.loadDataSource(Paths.get(CommonConfig.anomalyCsvPath), "Y");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TrainTestSplitter<Event> dataSplitter = new TrainTestSplitter<>(dataSource,0.5,1L);
        return new MutableDataset<>(dataSplitter.getTrain());
    }

    public static Model<Event> getSVMModel() {
        SVMParameters<Event> params = new SVMParameters<>(new SVMAnomalyType(SVMAnomalyType.SVMMode.ONE_CLASS), KernelType.RBF);
        params.setGamma(1.0);
        params.setNu(0.1);
        Trainer<Event> trainer = new LibSVMAnomalyTrainer(params);
        return trainer.train(getTrainingData());
    }

    public static Model<Event> getLinearModel() {
        LibLinearAnomalyTrainer trainer = new LibLinearAnomalyTrainer();
        return trainer.train(getTrainingData());
    }
}
