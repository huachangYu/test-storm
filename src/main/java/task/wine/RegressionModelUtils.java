package task.wine;

import org.tribuo.DataSource;
import org.tribuo.Model;
import org.tribuo.MutableDataset;
import org.tribuo.Trainer;
import org.tribuo.common.tree.RandomForestTrainer;
import org.tribuo.data.csv.CSVIterator;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.evaluation.TrainTestSplitter;
import org.tribuo.math.optimisers.AdaGrad;
import org.tribuo.math.optimisers.SGD;
import org.tribuo.regression.RegressionFactory;
import org.tribuo.regression.Regressor;
import org.tribuo.regression.ensemble.AveragingCombiner;
import org.tribuo.regression.rtree.CARTRegressionTrainer;
import org.tribuo.regression.rtree.impurity.MeanSquaredError;
import org.tribuo.regression.sgd.fm.FMRegressionTrainer;
import org.tribuo.regression.sgd.linear.LinearSGDTrainer;
import org.tribuo.regression.sgd.objectives.SquaredLoss;
import org.tribuo.regression.slm.ElasticNetCDTrainer;
import org.tribuo.regression.xgboost.XGBoostRegressionTrainer;
import smile.regression.RandomForest;
import task.common.CommonConfig;

import java.io.IOException;
import java.nio.file.Paths;

import static org.tribuo.common.tree.AbstractCARTTrainer.MIN_EXAMPLES;

public class RegressionModelUtils {
    public static String[] dataHeadersWithLabel = new String[] {"fixed acidity","volatile acidity",
            "citric acid","residual sugar","chlorides","free sulfur dioxide",
            "total sulfur dioxide","density","pH","sulphates","alcohol","quality"};
    public static String[] dataHeaders = new String[] {"fixed acidity","volatile acidity",
            "citric acid","residual sugar","chlorides","free sulfur dioxide",
            "total sulfur dioxide","density","pH","sulphates","alcohol"};
    public static String LabelColumn = "quality";

    public static MutableDataset<Regressor> getTrainingData() {
        RegressionFactory regressionFactory = new RegressionFactory();
        CSVLoader<Regressor> csvLoader = new CSVLoader<>(',', CSVIterator.QUOTE, regressionFactory);
        DataSource<Regressor> dataSource;
        try {
            dataSource = csvLoader.loadDataSource(Paths.get(CommonConfig.wineCsvPath), LabelColumn);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TrainTestSplitter<Regressor> dataSplitter = new TrainTestSplitter<>(dataSource,0.5,1L);
        return new MutableDataset<>(dataSplitter.getTrain());
    }

    public static Model<Regressor> getLinearSGDModel() {
        MutableDataset<Regressor> trainData = getTrainingData();
        LinearSGDTrainer trainer = new LinearSGDTrainer(
                new SquaredLoss(), // loss function
                SGD.getLinearDecaySGD(0.01),
                100,                // number of training epochs
                trainData.size()/4,// logging interval
                1,                 // minibatch size
                1L                 // RNG seed
        );
        return trainer.train(trainData);
    }

    public static Model<Regressor> getCARTRegressionModel() {
        CARTRegressionTrainer model = new CARTRegressionTrainer(6);
        return model.train(getTrainingData());
    }

    public static Model<Regressor> getElasticNetModel() {
        ElasticNetCDTrainer model = new ElasticNetCDTrainer(1.0, 0.5);
        return model.train(getTrainingData());
    }

    public static Model<Regressor> getFMModel() {
        FMRegressionTrainer model = new FMRegressionTrainer(new SquaredLoss(),
                new AdaGrad(0.1, 0.1), 5, 1000, Trainer.DEFAULT_SEED,
                1, 0.1, true);
        return model.train(getTrainingData());
    }

    public static Model<Regressor> getRandomForestModel() {
        CARTRegressionTrainer subsamplingTree = new CARTRegressionTrainer(Integer.MAX_VALUE,
                MIN_EXAMPLES, 0.0f, 0.5f,
                false, new MeanSquaredError(), Trainer.DEFAULT_SEED);
        RandomForestTrainer<Regressor> model = new RandomForestTrainer<>(subsamplingTree, new AveragingCombiner(), 10);
        return model.train(getTrainingData());
    }

}
