package task.spout;

import com.google.gson.Gson;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import task.model.DatasetParam;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

public class Source extends BaseRichSpout {
    private static final Gson GSON = new Gson();
    private static long id = 0;
    SimpleDateFormat dataformat = new SimpleDateFormat("hh:mm:ss.S");
    SpoutOutputCollector collector;
    Random rand;
    long preTime = -1;
    long startTime = -1;
    long count = 0;
    long total = 0;

    int qps;

    public Source(int qps) {
        this.qps = qps;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        long current = System.currentTimeMillis();
        if (preTime == -1) {
            preTime = current;
        }
        if (startTime == -1) {
            startTime = current;
        }
        if (current - preTime >= 1000) {
            String time = dataformat.format(new Date(Long.parseLong(String.valueOf(current))));
            System.out.printf("[%s] [Source] time: %d, total:%d, avg:%.2f. periodCnt:%d\n",
                    time, current, total, (double)(1000 * total) / (double)(current - startTime), count);
            total += count;
            count = 0;
            preTime = current;
        }

        int interval = qps / 10;
        int cur = 0;

        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            int end = i == 9 ? qps : cur + interval;
            while (cur < end) {
                count++;
                int flag = rand.nextInt(100);
                String type = "A"; // 60%
                if (60 <= flag && flag < 90) {
                    type = "B"; // 30%
                } else if (90 <= flag && flag < 99) {
                    type = "C"; // 9%
                } else if (flag == 99) {
                    type = "D"; // 1%
                }
                int size = rand.nextInt(100) + 1;
                float fraction = rand.nextFloat();
                long seed = rand.nextLong();
                String data = GSON.toJson(new DatasetParam(id++, type, System.currentTimeMillis(), size, fraction, seed));
                collector.emit(new Values(data));
                cur++;
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}
