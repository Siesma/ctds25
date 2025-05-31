package query;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedDeque;

public class TransactionManager extends Thread {
    private final ExecutionEngine executionEngine;

    private final ConcurrentLinkedDeque<Instruction> pendingTransactions;

    private volatile boolean running;

    public TransactionManager(ExecutionEngine engine) {
        this.executionEngine = engine;
        this.pendingTransactions = new ConcurrentLinkedDeque<Instruction>();
        this.running = true;
    }

    public synchronized void runTransaction(Instruction... instructions) {
        for(Instruction instruction : instructions) {
            instruction.setQueueTime(Timestamp.get());
            this.pendingTransactions.add(instruction);
        }
    }

    @Override
    public void run() {
        while (running) {
            Instruction nextInstruction = pendingTransactions.poll();
            if(nextInstruction == null) {
                try {
                    Thread.sleep(1);
                    continue;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            runInstruction(nextInstruction);
        }
    }

    private void runInstruction (Instruction instruction) {
        if(instruction == null) {
            System.exit(1);
            return;
        }
        instruction.setSetupTime(Timestamp.get());
        executionEngine.executeSafe(instruction);
        instruction.setExecutionTime(Timestamp.get());
    }

    public boolean isDone () {
        return pendingTransactions.isEmpty();
    }

    public void finish () {
        this.running = false;
    }

}
