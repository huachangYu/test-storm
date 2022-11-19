package task.smoke;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import task.common.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OutputBolt extends BaseBasicBolt {
    private static Lock lock = new ReentrantLock();
    private long boltStartTime = -1;
    private Map<String, Long> periodCount = new HashMap<>();
    private Map<String, Long> totalCount = new HashMap<>();
    private Map<String, Long> periodCost = new HashMap<>();
    private Map<String, Long> totalCost = new HashMap<>();
    private long preTime = -1;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long current = System.currentTimeMillis();
        if (boltStartTime <= 0) {
            boltStartTime = current;
            preTime = current;
        }
        Long start = input.getLongByField("start");
        String modelType = input.getStringByField("modelType");
        String type = input.getStringByField("type");
        List<String> predictions = (List<String>) input.getValueByField("predictions");
        int alarm = 0;
        int expect = 0;
        int unknown = 0;
        for (String pred : predictions) {
            if (pred.equals("1")) {
                alarm++;
            } else if (pred.equals("0")) {
                expect++;
            } else {
                unknown++;
            }
        }
        periodCount.put(modelType, periodCount.getOrDefault(modelType, 0L) + 1);
        totalCount.put(modelType, totalCount.getOrDefault(modelType, 0L) + 1);
        long cost = current - start;
        periodCost.put(modelType, periodCost.getOrDefault(modelType, 0L) + cost);
        totalCost.put(modelType, totalCost.getOrDefault(modelType, 0L) + cost);
//        System.out.printf("[Output-Latency] time=%d, cost=%d, model=%s, alarm=%d, expect=%d, unknown=%d\n",
//                current - boltStartTime, current - start, modelType, alarm, expect, unknown);
        if (current - preTime >= 1000) {
            lock.lock();
            if (current - preTime >= 1000 && periodCount.size() != 0) {
                double avgCost = totalCount.keySet().stream()
                        .map(t -> totalCost.getOrDefault(t, 0L).doubleValue() / totalCount.getOrDefault(t, 1L).doubleValue())
                        .max(Double::compareTo).get();
                double avgCnt = totalCount.keySet().stream()
                        .map(t -> 1000 * totalCount.getOrDefault(t, 0L).doubleValue() /  (double)(current - boltStartTime))
                        .min(Double::compareTo).get();
                double avgPeriodCost = periodCount.keySet().stream()
                        .map(t -> periodCost.getOrDefault(t, 0L).doubleValue() / periodCount.getOrDefault(t, 1L).doubleValue())
                        .max(Double::compareTo).get();
                long avgPeriodCount = periodCount.values().stream().min(Long::compareTo).get();

                System.out.printf("[Output-Throughput] time=%d, avgCost=%.2f, avgCnt=%.2f, " +
                                "periodCnt=%d, periodAvgCost=%.2f, cpu=%.2f\n",
                        current - boltStartTime, avgCost, avgCnt,
                        avgPeriodCount, avgPeriodCost,
                        Utils.systemRecorder.getAndRecordCpuLoad() * 100);
                periodCost.clear();
                periodCount.clear();
                preTime = current;
            }
            lock.unlock();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
