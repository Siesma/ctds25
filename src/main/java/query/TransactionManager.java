package query;

import java.util.*;

public class TransactionManager {

    /*
    Append conditions to check for conflicts
     */

    private final ExecutionEngine executionEngine;

    public TransactionManager(ExecutionEngine engine) {
        this.executionEngine = engine;
    }

    public void runTransaction(Instruction... instructions) {
        Arrays.asList(instructions).forEach(e -> {
            e.setQueueTime(Timestamp.get());
            executionEngine.execute(e);
        });
    }

    public synchronized String getUUIDByString(String input) {
        return "";
    }

    public synchronized String obtainNewUUID() {
        return UUID.randomUUID().toString();
    }

}
