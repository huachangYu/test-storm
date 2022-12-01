package task.common;

public class CommonConfig {
    public static volatile boolean isLocal = false;
    public static volatile String anomalyCsvPath = isLocal ? "data/anomaly/anomaly_detection.csv"
            : "/share/data/anomaly/anomaly_detection.csv";
    public static volatile String smokeCsvPath = isLocal ? "data/smoke/dataset.csv"
            : "/share/data/smoke/dataset.csv";
    public static volatile String wineCsvPath = isLocal ? "data/wine/winequality-white.csv"
            : "/share/data/wine/winequality-white.csv";
    public static volatile String qpsFilePath = isLocal ? "data/qps.tmp" : "/share/qps.tmp";
}
