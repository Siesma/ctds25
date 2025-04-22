package query;

import query.ExecutionEngine;
import query.OperationType;

import java.util.*;

public class TransactionManager {
    private final ExecutionEngine executionEngine;

    public TransactionManager(ExecutionEngine engine) {
        this.executionEngine = engine;
    }

    public void runTransaction(Instruction... instructions) {
        Arrays.asList(instructions).forEach(executionEngine::execute);
    }

    public static class Instruction {
        public OperationType opType;
        public String tableName;
        public Map<String, Integer> rowData;

        public Instruction(OperationType opType, String tableName, Map<String, String> rowData) {
            this.opType = opType;
            this.tableName = tableName;
            this.rowData = rowData;
        }
    }

    public synchronized String getUUIDByString(String input) {
        return "";
    }

    public synchronized String obtainNewUUID() {
        return UUID.randomUUID().toString();
    }

}
