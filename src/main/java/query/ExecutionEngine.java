package query;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class ExecutionEngine {

    private final DataManager dataManager;
    private final Stack<Instruction> commitHistory;
    private final Stack<Instruction> versionHistory;

    public ExecutionEngine(DataManager dataManager) {
        this.dataManager = dataManager;
        this.commitHistory = new Stack<>();
        this.versionHistory = new Stack<>();
    }

    public int execute(Instruction instruction, QueryType opType, String table, Map<String, Integer> row) {
        instruction.setPreOperation(dataManager.getSnapshot());

        Map<String, Map<String, Integer>> delta = null;
        Map<String, Map<String, Integer>> post = null;

        switch (opType) {
            case UPDATE:
                delta = dataManager.updateDelta(table, row, instruction.getPreOperation());
                break;
            case DELETE:
                delta = dataManager.deleteDelta(table, row, instruction.getPreOperation());
                break;
            case INSERT:
                delta = dataManager.insertDelta(table, row, instruction.getPreOperation());
                break;
            case GET:
                Map<String, Integer> results = dataManager.select(table);
                System.out.println("SELECT results for " + table + ": " + results);
                return 0;
            case VISUALISE:
                dataManager.visualiseDataStore(table);
                return 0;
            case ROLLBACK:
                rollback(instruction, table);
                return 0;
            case COMMIT:
                while (!versionHistory.isEmpty()) {
                    Instruction toCommit = versionHistory.pop();
                    try {
                        dataManager.applySnapshot(toCommit);
                        commitHistory.push(toCommit);
                    } catch (IllegalStateException e) {
                        System.err.println("Commit failed due to conflict: " + e.getMessage());
                        return 1;
                    }
                }
                return 0;
            default:
                System.out.println("Operation not supported yet: " + opType);
                return 1;
        }

        post = applyDeltaToSnapshot(instruction.getPreOperation(), delta);

        instruction.setDelta(delta);
        instruction.setPostOperation(post);

        versionHistory.push(instruction);
        commitHistory.push(instruction);

        return 0;
    }

    public int execute(Instruction instruction) {
        return execute(instruction, instruction.opType, instruction.tableName, instruction.rowData);
    }

    public void rollback(Instruction instruction, String key) {
        if (commitHistory.isEmpty()) {
            return;
        }

        commitHistory.push(instruction);
        Stack<Instruction> relevantCommits = new Stack<>();

        while (!commitHistory.isEmpty() && !commitHistory.peek().getUuid().toString().equals(key)) {
            relevantCommits.push(commitHistory.pop());
        }

        if (!commitHistory.isEmpty()) {
            Instruction rollBackInstruction = commitHistory.pop();
            dataManager.restoreSnapshot(rollBackInstruction);
        }
        
        while (!relevantCommits.isEmpty()) {
            Instruction instr = relevantCommits.pop();
            execute(instr);
        }
    }

    private Map<String, Map<String, Integer>> applyDeltaToSnapshot(
        Map<String, Map<String, Integer>> base,
        Map<String, Map<String, Integer>> delta
    ) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> entry : base.entrySet()) {
            result.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }

        for (Map.Entry<String, Map<String, Integer>> entry : delta.entrySet()) {
            String key = entry.getKey();
            Map<String, Integer> changes = entry.getValue();

            if (changes == null) {
                result.remove(key);
                continue;
            }

            result.putIfAbsent(key, new HashMap<>());
            Map<String, Integer> row = result.get(key);

            for (Map.Entry<String, Integer> change : changes.entrySet()) {
                if (change.getValue() == null) {
                    row.remove(change.getKey());
                } else {
                    row.put(change.getKey(), change.getValue());
                }
            }
        }

        return result;
    }
}
