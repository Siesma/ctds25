package query;

import java.util.Map;
import java.util.UUID;

public class Instruction {

    /*
    Order IDs, and maybe swap from uuid
    and append "status" variable
     */

    public QueryType opType;
    public String tableName;
    public Map<String, Integer> rowData;
    private final UUID uuid;

    private Map<String, Map<String, Integer>> snapshot;

    public Instruction(QueryType opType, String tableName, Map<String, Integer> rowData) {
        this.opType = opType;
        this.tableName = tableName;
        this.rowData = rowData;
        this.uuid = UUID.randomUUID();
    }

    public void setSnapshot(Map<String, Map<String, Integer>> snapshot) {
        this.snapshot = snapshot;
    }

    public Map<String, Map<String, Integer>> getSnapshot() {
        return snapshot;
    }

    @Override
    public String toString () {
        return String.format("%s %s {%s}", opType.toString(), tableName, rowData.toString().replaceAll("\n", ""));
    }

    public UUID getUuid() {
        return uuid;
    }


}
