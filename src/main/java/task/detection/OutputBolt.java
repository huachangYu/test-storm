package task.detection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.tribuo.anomaly.Event;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class OutputBolt extends BaseBasicBolt {
    SimpleDateFormat dataformat = new SimpleDateFormat("hh:mm:ss.S");
    private long boltStartTime = -1;
    private long preTime = -1;
    private long periodCnt = 0;
    private long totalCnt = 0;
    private long totalCost = 0;
    private long periodCost = 0;
    private long anomalyCount = 0;
    private long normalCount = 0;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long current = System.currentTimeMillis();
        if (preTime == -1) {
            preTime = current;
        }
        if (boltStartTime == -1) {
            boltStartTime = current;
        }
        Long id = (Long) input.getValueByField("id");
        Long eventTime = (Long) input.getValueByField("eventTime");
        List<Event.EventType> predictions = (List<Event.EventType>) input.getValueByField("predictions");
        for (Event.EventType prediction : predictions) {
            if (prediction == Event.EventType.ANOMALOUS) {
                anomalyCount++;
            } else if (prediction == Event.EventType.EXPECTED) {
                normalCount++;
            }
        }

        String time = dataformat.format(new Date(Long.parseLong(String.valueOf(current))));
        long cost = System.currentTimeMillis() - eventTime;
        periodCost += cost;
        totalCost += cost;
        totalCnt++;
        periodCnt++;
        System.out.printf("[%s] [Output-Latency] id:%d, size:%d, cost:%d\n", time, id, predictions.size(), cost);
        if (current - preTime >= 1000) {
            System.out.printf("[%s] [Output-Throughput] time=%d, avgCost=%.2f, avgCnt=%.2f, periodCnt=%d, periodAvgCost=%.2f\n",
                    time, current - boltStartTime,
                    (double) totalCost / (double) totalCnt,
                    (double)(1000 * totalCnt) / (double)(current - boltStartTime),
                    periodCnt,
                    (double) periodCost / (double) periodCnt);
            System.out.printf("anomalyCount:%d, normalCount:%d\n", anomalyCount, normalCount);
            periodCost = 0;
            periodCnt = 0;
            anomalyCount = 0;
            normalCount = 0;
            preTime = current;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}