package query;

import java.util.Map;
import java.util.List;

public class ExecutionEngine {

    private final DataManager dataManager;

    public ExecutionEngine(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public void execute(OperationType opType, String table, Map<String, Integer> row) {
        switch (opType) {
            case UPDATE:
                break;
            case DELETE:
                break;
            case INSERT:
                dataManager.insert(table, row);
                break;
            case SELECT:
                Map<String, Integer> results = dataManager.select(table);
                System.out.println("SELECT results for " + table + ": " + results);
                break;
            default:
                System.out.println("Operation not supported yet: " + opType);
        }
    }

    public void execute(TransactionManager.Instruction instruction) {
        // TODO: Verify correctness of instruction
        execute(instruction.opType, instruction.tableName, instruction.rowData);
    }
}
