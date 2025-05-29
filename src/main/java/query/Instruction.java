package query;

import java.util.Map;
import java.util.UUID;

public class Instruction {

    public QueryType opType;
    public String tableName;
    public Map<String, Integer> rowData;
    private final UUID uuid;
    private Timestamp queueTime;
    private Timestamp setupTime;
    private Timestamp executionTime;

    private Map<String, Map<String, Integer>> preOperation;  // snapshot before operation
    private Map<String, Map<String, Integer>> postOperation; // full result after applying delta
    private Map<String, Map<String, Integer>> delta;         // only what changed

    public Instruction(QueryType opType, String tableName, Map<String, Integer> rowData) {
        this.opType = opType;
        this.tableName = tableName;
        this.rowData = rowData;
        this.uuid = UUID.randomUUID();
    }

    public UUID getUuid() {
        return uuid;
    }

    public Map<String, Map<String, Integer>> getPreOperation() {
        return preOperation;
    }

    public Instruction setPreOperation(Map<String, Map<String, Integer>> preOperation) {
        this.preOperation = preOperation;
        return this;
    }

    public Map<String, Map<String, Integer>> getPostOperation() {
        return postOperation;
    }

    public Instruction setPostOperation(Map<String, Map<String, Integer>> postOperation) {
        this.postOperation = postOperation;
        return this;
    }

    public Map<String, Map<String, Integer>> getDelta() {
        return delta;
    }

    public Instruction setDelta(Map<String, Map<String, Integer>> delta) {
        this.delta = delta;
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s %s {%s}", opType, tableName, rowData.toString().replaceAll("\n", ""));
    }

    public Timestamp getQueueTime() {
        return queueTime;
    }

    public void setQueueTime(Timestamp queueTime) {
        this.queueTime = queueTime;
    }

    public Timestamp getSetupTime() {
        return setupTime;
    }

    public void setSetupTime(Timestamp setupTime) {
        this.setupTime = setupTime;
    }

    public Timestamp getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(Timestamp executionTime) {
        this.executionTime = executionTime;
    }
}
