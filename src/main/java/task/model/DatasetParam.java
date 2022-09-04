package task.model;

import java.util.UUID;

public class DatasetParam {
    public DatasetParam(Long id, String type, long eventTime, int size, float fractionAnomalous, long seed) {
        this.id = id;
        this.uuid = UUID.randomUUID();
        this.type = type;
        this.eventTime = eventTime;
        this.size = size;
        this.fractionAnomalous = fractionAnomalous;
        this.seed = seed;
        this.message = "";
        this.from = "spout";
    }

    public Long id;
    UUID uuid;
    public String type;
    public long eventTime;
    public int size;
    public float fractionAnomalous;
    public long seed;
    public String message;
    public String from;
}
