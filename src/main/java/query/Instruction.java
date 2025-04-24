package query;

import java.util.Map;

public class Instruction {

    public OperationType opType;
    public String tableName;
    public Map<String, Integer> rowData;

    public Instruction(OperationType opType, String tableName, Map<String, Integer> rowData) {
        this.opType = opType;
        this.tableName = tableName;
        this.rowData = rowData;
    }

    @Override
    public String toString () {
        return String.format("%s %s {%s}", opType.toString(), tableName, rowData.toString().replaceAll("\n", ""));
    }


}
