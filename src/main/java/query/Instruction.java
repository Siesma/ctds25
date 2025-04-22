package query;

import java.util.Map;

public class Instruction {

    public OperationType opType;
    public String tableName;
    public Map<String, String> rowData;

    public Instruction(OperationType opType, String tableName, Map<String, String> rowData) {
        this.opType = opType;
        this.tableName = tableName;
        this.rowData = rowData;
    }


}
