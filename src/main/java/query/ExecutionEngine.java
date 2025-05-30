package query;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class ExecutionEngine {

    private final IDataManager dataManager;
    private final Stack<Instruction> commitHistory;
    private final Stack<Instruction> versionHistory;

    public ExecutionEngine(IDataManager dataManager) {
        this.dataManager = dataManager;
        this.commitHistory = new Stack<>();
        this.versionHistory = new Stack<>();
    }

    // executes the instruction, please dont use this method and use the overloaded "execute" method so that we can obtain timing values
    public int execute(Instruction instruction) {
        instruction.setSetupTime(Timestamp.get());
        int result = execute(instruction, instruction.opType, instruction.tableName, instruction.rowData);
        instruction.setExecutionTime(Timestamp.get());
        return result;
    }

    // Main execution logic
    private int execute(Instruction instruction, QueryType opType, String table, Map<String, Integer> row) {
        try {
            instruction.setPreOperation(dataManager.getSnapshot());
        } catch (Exception e) {
            System.out.println("Snapshot error: " + e.getMessage());
        }

        Map<String, Map<String, Integer>> delta = null;
        Map<String, Map<String, Integer>> post = null;

        switch (opType) {
            case INSERT:
                delta = dataManager.insert(table, row, instruction.getPreOperation());
                break;
            case UPDATE:
                delta = dataManager.update(table, row, instruction.getPreOperation());
                break;
            case INCREMENT:
                delta = dataManager.increment(table, row, instruction.getPreOperation());
                break;
            case DECREMENT:
                delta = dataManager.decrement(table, row, instruction.getPreOperation());
                break;
            case DELETE:
                delta = dataManager.delete(table, row, instruction.getPreOperation());
                break;
            case GET:
                Map<String, Integer> results = dataManager.select(table);
                System.out.println("SELECT results for " + table + ": " + results);
                return 0;
            case VISUALISE:
                dataManager.visualiseDataStore(table);
                return 0;
            case COMMIT:
                handleCommit(table);
                return 0;
            case ROLLBACK:
                handleRollback(instruction, table);
                return 0;
            default:
                System.out.println("Operation not supported yet: " + opType);
                return 1;
        }

        try {
            post = applyDeltaToSnapshot(instruction.getPreOperation(), delta);
            instruction.setDelta(delta);
            instruction.setPostOperation(post);
        } catch (Exception e) {
            System.out.println("Delta application error: " + e.getMessage());
        }

        versionHistory.push(instruction);
        return 0;
    }

    private void handleCommit(String commitId) {
        while (!versionHistory.isEmpty()) {
            Instruction toCommit = versionHistory.pop();
            try {
                dataManager.applySnapshot(toCommit);
                toCommit.setCommitID(commitId); // Tag the instruction with the commit ID
                commitHistory.push(toCommit);
            } catch (Exception e) {
                System.out.println("Commit apply error: " + e.getMessage());
                break;
            }
        }
    }


    public synchronized void handleRollback(Instruction rollbackInstruction, String rollbackId) {
        if (commitHistory.isEmpty()) return;

        commitHistory.push(rollbackInstruction);
        Stack<Instruction> toReplay = new Stack<>();
        boolean found = false;

        while (!commitHistory.isEmpty()) {
            Instruction top = commitHistory.pop();
            if (rollbackId.equals(top.getCommitID())) {
                dataManager.restoreSnapshot(top);
                found = true;
                break;
            } else {
                toReplay.push(top);
            }
        }

        while (!toReplay.isEmpty()) {
            execute(toReplay.pop());
        }

        if (!found) {
            System.out.println("Rollback failed: no commit with ID " + rollbackId);
        }
    }

    // Applies a delta to a snapshot to produce a post-operation view
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
