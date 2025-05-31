package database;

import query.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static String type;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: <benchmark_dir> <num_threads> <benchmark_type>");
            System.exit(1);
        }

        String benchmarkDir = args[0];
        int numThreads = Integer.parseInt(args[1]);
        type = args[2];
        IDataManager dataManager;
        switch (type) {
            case "Lock":
                dataManager = new DataManagerLock();
                break;
            case "MVCC":
                 dataManager = new DataManagerMVCC();
                 break;
            default:
                System.out.println("Not supported benchmark type: " + type);
                return;
        }
        ExecutionEngine engine = new ExecutionEngine(dataManager);
        TransactionManager tm = new TransactionManager(engine);
        tm.start();
        Parser parser = new Parser();

        List<Thread> threads = new ArrayList<>();
        List<Instruction> allInstructions = Collections.synchronizedList(new ArrayList<>());

        List<List<Instruction>> threadInstructions = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            threadInstructions.add(i, new ArrayList<>());
            String filePath = String.format("%s/T%d", benchmarkDir, i + 1);
            int finalI = i;
            Thread t = new Thread(() -> {

                try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue; // should never occur in our samples

                        Instruction instr = parser.parse(line);
                        if (instr != null) {
                            threadInstructions.get(finalI).add(instr);
                        } else {
                            System.err.println("Instruction is null for: " + line);
                            System.exit(1);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + filePath);
                    e.printStackTrace();
                    return;
                }
                threadInstructions.get(finalI).forEach(tm::runTransaction);
            });

            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for(List<Instruction> l : threadInstructions) {
            allInstructions.addAll(l);
        }

        System.out.println("\nFinal state after all threads complete:");
        tm.runTransaction(parser.parse("VISUALISE *"));
        while (!tm.isDone()) {
            try {
                Thread.sleep(1);

            } catch (InterruptedException e) {

            }
        }
        tm.finish();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        analyzeTimings(allInstructions);
    }

    private static void analyzeTimings(List<Instruction> instructions) {
        System.out.printf("--- Timing Analysis (in microseconds) for %s---%n", type);
        long totalQueue = 0, totalSetup = 0, totalTotal = 0;
        long minTotal = Long.MAX_VALUE, maxTotal = Long.MIN_VALUE;
        long minQueued = Long.MAX_VALUE;
        long maxExecuted = Long.MIN_VALUE;

        int count = 0;
        int amountWaited = 0;
        for (Instruction instr : instructions) {
            if (instr == null || instr.getQueueTime() == null || instr.getSetupTime() == null || instr.getExecutionTime() == null) {
                continue;
            }

            long q = instr.getQueueTime().getTime();
            long s = instr.getSetupTime().getTime();
            long e = instr.getExecutionTime().getTime();

            if (s < q || e < s) {
                System.out.println(s + " " + q + " " + e);
                System.exit(1);
                continue; // skip invalid, should never be
            }
            if(s - q >= 1e-4) {
                amountWaited++;
            }
            long queueToSetup = s - q;
            long setupToExec = e - s;
            long total = e - q;

            totalQueue += queueToSetup;
            totalSetup += setupToExec;
            totalTotal += total;

            minTotal = Math.min(minTotal, total);
            maxTotal = Math.max(maxTotal, total);

            minQueued = Math.min(minQueued, q);
            maxExecuted = Math.max(maxExecuted, e);

            count++;
        }

        if (count == 0) {
            System.out.println("No valid instruction timings recorded.");
            return;
        }
        if(count != 4500) {
            System.out.println(instructions.size());
        }

        double effectiveWallClockMs = (maxExecuted - minQueued) / 1000.0;

        System.out.printf("Total instructions: %d%n", count);
        System.out.printf("Instruction amount that had to wait for others: %d%n", amountWaited);
        System.out.printf("Avg Queue→Setup Time: %.2f µs%n", totalQueue / (double) count);
        System.out.printf("Avg Setup→Exec Time: %.2f µs%n", totalSetup / (double) count);
        System.out.printf("Avg Total Time: %.2f µs%n", totalTotal / (double) count);
        System.out.printf("Min Total Time: %d µs%n", minTotal);
        System.out.printf("Max Total Time: %d µs%n", maxTotal);
        System.out.printf("Effective wall-clock time: %.2f ms%n", effectiveWallClockMs);
    }

}
