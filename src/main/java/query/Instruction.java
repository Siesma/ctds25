package query;

import java.sql.SQLOutput;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class Instruction {

    private static final AtomicInteger counter = new AtomicInteger(0);
    public QueryType opType;
    public String tableName;
    public Map<String, Integer> rowData;
    private final UUID uuid;
    public final int id;
    private Timestamp queueTime;
    private Timestamp setupTime;
    private Timestamp executionTime;
    // this keeps track to determine to which commit this instruction belong
    private String commitID;

    private Map<String, Map<String, Integer>> preOperation;  // snapshot before operation
    private Map<String, Map<String, Integer>> postOperation; // full result after applying delta
    private Map<String, Map<String, Integer>> delta;         // only what changed

    public Instruction(QueryType opType, String tableName, Map<String, Integer> rowData) {
        this.opType = opType;
        this.tableName = tableName;
        this.rowData = rowData;
        this.uuid = UUID.randomUUID();
        this.id = counter.incrementAndGet();
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
        return String.format("%s %s %s {%s}\t;%s;%s;%s", id, opType, tableName, rowData == null ? "<>" :
            rowData.toString().replaceAll("\n", ""), queueTime.toString(), setupTime.toString(), executionTime.toString());
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

    public String getCommitID() {
        return commitID;
    }

    public void setCommitID(String commitID) {
        this.commitID = commitID;
    }

    public int printMissingData() {
        if (this.setupTime != null && this.queueTime != null && this.executionTime != null) {
            return 0;
        }
        System.out.printf("Instruction with ID %s is invalid: %s\n", id, toString());
        if (this.setupTime == null) {
            System.out.printf("\tSetuptime is null%n");
        }
        if (this.queueTime == null) {
            System.out.printf("\tQueuetime is null%n");
        }
        if (this.executionTime == null) {
            System.out.printf("\tExecutiontime is null%n");
        }
        return 1;
    }

}
