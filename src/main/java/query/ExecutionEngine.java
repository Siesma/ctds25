package query;

import java.util.Map;

public class ExecutionEngine {

    private final DataManager dataManager;

    public ExecutionEngine(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public void execute(OperationType opType, String table, Map<String, Integer> row) {
        switch (opType) {
            case UPDATE:
                dataManager.update(table, row);
                break;
            case DELETE:
                dataManager.delete(table, row);
                break;
            case INSERT:
                dataManager.insert(table, row);
                break;
            case GET:
                Map<String, Integer> results = dataManager.select(table);
                System.out.println("SELECT results for " + table + ": " + results);
                break;
            case VISUALISE:
                dataManager.visualiseDataStore(table);
            default:
                System.out.println("Operation not supported yet: " + opType);
        }
    }

    public void execute(Instruction instruction) {
        // TODO: Verify correctness of instruction
        execute(instruction.opType, instruction.tableName, instruction.rowData);
    }
}
