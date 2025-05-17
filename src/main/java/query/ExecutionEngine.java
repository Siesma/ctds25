package query;

import java.util.Map;
import java.util.Stack;

public class ExecutionEngine {

    private final DataManager dataManager;


    private final Stack<Instruction> commitHistory;

    public ExecutionEngine(DataManager dataManager) {
        this.dataManager = dataManager;
        this.commitHistory = new Stack<>();
    }

    /*
    Return a status integer to show whether an instruction has been completed correctly
     */
    public int execute(Instruction instruction, QueryType opType, String table, Map<String, Integer> row) {
        commitHistory.push(instruction);
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
            case ROLLBACK:
                rollback(instruction, table);
            default:
                System.out.println("Operation not supported yet: " + opType);
                return 1;
        }
        return 0;
    }

    public int execute(Instruction instruction) {
        // TODO: Verify correctness of instruction
        return execute(instruction, instruction.opType, instruction.tableName, instruction.rowData);
    }

    public void rollback(Instruction instruction, String key) {
        if(commitHistory.isEmpty()) {
            return;
        }
        commitHistory.push(instruction);
        Stack<Instruction> relevantCommits = new Stack<>();
        while (!commitHistory.isEmpty() && !commitHistory.peek().getUuid().toString().equals(key)) {
            relevantCommits.push(commitHistory.pop());
        }
        Instruction rollBackInstruction = commitHistory.pop();
        dataManager.restoreSnapshot(rollBackInstruction);
        relevantCommits.forEach(this::execute);
    }

}
