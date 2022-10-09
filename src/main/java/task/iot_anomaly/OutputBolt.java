package task.iot_anomaly;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.tribuo.anomaly.Event;

import java.util.List;

public class OutputBolt extends BaseBasicBolt {
    private long boltStartTime = -1;
    private long preTime = -1;
    private long periodCount = 0;
    private long periodCost = 0;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long current = System.currentTimeMillis();
        if (boltStartTime <= 0) {
            boltStartTime = current;
            preTime = current;
        }
        Long start = input.getLongByField("start");
        String type = input.getStringByField("type");
        List<Event.EventType> predictions = (List<Event.EventType>) input.getValueByField("predictions");
        int anomaly = 0;
        int expect = 0;
        int unknown = 0;
        for (Event.EventType pred : predictions) {
            if (pred == Event.EventType.ANOMALOUS) {
                anomaly++;
            } else if (pred == Event.EventType.EXPECTED) {
                expect++;
            } else {
                unknown++;
            }
        }
        periodCount++;
        long cost = current - start;
        periodCost += cost;
//        System.out.printf("time=%d, cost=%d, anomaly=%d, expect=%d, unknown=%d\n",
//                current - boltStartTime, current - start, anomaly, expect, unknown);
        if (current - preTime >= 1000) {
            System.out.printf("[Output-Throughput] time=%d, count=%d, avgCost=%.2f\n",
                    current - boltStartTime, periodCount, (double) periodCost / (double) periodCount);
            periodCost = 0;
            periodCount = 0;
            preTime = current;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
