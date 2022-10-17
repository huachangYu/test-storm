package task.common;

public class CommonConfig {
    public static final boolean isLocal = true;
    public static final String anomalyCsvPath = isLocal ? "data/anomaly/anomaly_detection.csv"
            : "/share/anomaly/anomaly_detection.csv";
    public static final String smokeCsvPath = isLocal ? "data/smoke/dataset.csv"
            : "/share/smoke/dataset.csv";
}
