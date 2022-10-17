package task.smoke;

import org.tribuo.DataSource;
import org.tribuo.Model;
import org.tribuo.MutableDataset;
import org.tribuo.Trainer;
import org.tribuo.classification.Label;
import org.tribuo.classification.LabelFactory;
import org.tribuo.classification.dtree.CARTClassificationTrainer;
import org.tribuo.classification.libsvm.LibSVMClassificationTrainer;
import org.tribuo.classification.libsvm.SVMClassificationType;
import org.tribuo.classification.sgd.linear.LogisticRegressionTrainer;
import org.tribuo.common.libsvm.KernelType;
import org.tribuo.common.libsvm.SVMParameters;
import org.tribuo.data.csv.CSVIterator;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.evaluation.TrainTestSplitter;
import task.common.CommonConfig;

import java.io.IOException;
import java.nio.file.Paths;

public class PredictionModelUtils {
    public static String[] dataHeadersWithLabel = new String[] {"Temperature[C]", "Humidity[%]", "TVOC[ppb]", "eCO2[ppm]",
            "Raw H2", "Raw Ethanol", "Pressure[hPa]", "PM1.0", "PM2.5", "NC0.5", "NC1.0", "NC2.5",
            "CNT", "Fire Alarm"};
    public static String[] dataHeaders = new String[] {"Temperature[C]", "Humidity[%]", "TVOC[ppb]", "eCO2[ppm]",
            "Raw H2", "Raw Ethanol", "Pressure[hPa]", "PM1.0", "PM2.5", "NC0.5", "NC1.0", "NC2.5",
            "CNT"};
    public static String LabelColumn = "Fire Alarm";

    public static MutableDataset<Label> getTrainingData() {
        LabelFactory labelFactory = new LabelFactory();
        CSVLoader<Label> csvLoader = new CSVLoader<>(',', CSVIterator.QUOTE, labelFactory);
        DataSource<Label> dataSource = null;
        try {
            dataSource = csvLoader.loadDataSource(Paths.get(CommonConfig.smokeCsvPath), LabelColumn);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TrainTestSplitter<Label> dataSplitter = new TrainTestSplitter<>(dataSource,0.5,1L);
        return new MutableDataset<>(dataSplitter.getTrain());
    }

    public static Model<Label> getSvmModel() {
        SVMParameters<Label> params = new SVMParameters<>(
                new SVMClassificationType(SVMClassificationType.SVMMode.NU_SVC), KernelType.RBF);
        Trainer<Label> trainer = new LibSVMClassificationTrainer(params);
        return trainer.train(getTrainingData());
    }

    public static Model<Label> getLogisticModel() {
        Trainer<Label> trainer = new LogisticRegressionTrainer();
        return trainer.train(getTrainingData());
    }

    public static Model<Label> getCartModel() {
        Trainer<Label> trainer = new CARTClassificationTrainer();
        return trainer.train(getTrainingData());
    }

}
