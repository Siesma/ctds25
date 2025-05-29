package database;

import query.*;

import java.io.*;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: <benchmark_dir> <num_threads>");
            System.exit(1);
        }

        String benchmarkDir = args[0];
        int numThreads = Integer.parseInt(args[1]);

        IDataManager dataManager = new DataManagerLock();
        ExecutionEngine engine = new ExecutionEngine(dataManager);
        TransactionManager tm = new TransactionManager(engine);
        Parser parser = new Parser();

        List<Thread> threads = new ArrayList<>();
        List<Instruction> allInstructions = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numThreads; i++) {
            String filePath = String.format("%s/T%d", benchmarkDir, i + 1);
            Thread t = new Thread(() -> {
                List<Instruction> instructions = new ArrayList<>();

                try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        Instruction instr = parser.parse(line);
                        if (instr != null) {
                            instructions.add(instr);
                            allInstructions.add(instr);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + filePath);
                    e.printStackTrace();
                    return;
                }

                instructions.forEach(tm::runTransaction);
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

        System.out.println("\nFinal state after all threads complete:");
        engine.execute(parser.parse("VISUALISE *"));

        analyzeTimings(allInstructions);
    }

    private static void analyzeTimings(List<Instruction> instructions) {
        System.out.println("\n--- Timing Analysis (in microseconds) ---");

        long totalQueue = 0, totalSetup = 0, totalTotal = 0;
        long minTotal = Long.MAX_VALUE, maxTotal = Long.MIN_VALUE;
        long minQueued = Long.MAX_VALUE;
        long maxExecuted = Long.MIN_VALUE;

        int count = 0;

        for (Instruction instr : instructions) {
            if (instr == null || instr.getQueueTime() == null || instr.getSetupTime() == null || instr.getExecutionTime() == null)
                continue;

            long q = instr.getQueueTime().getTime();
            long s = instr.getSetupTime().getTime();
            long e = instr.getExecutionTime().getTime();

            if (s < q || e < s) continue; // skip invalid, should never be

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

        double effectiveWallClockMs = (maxExecuted - minQueued) / 1000.0;

        System.out.printf("Total instructions: %d%n", count);
        System.out.printf("Avg Queue→Setup Time: %.2f µs%n", totalQueue / (double) count);
        System.out.printf("Avg Setup→Exec Time: %.2f µs%n", totalSetup / (double) count);
        System.out.printf("Avg Total Time: %.2f µs%n", totalTotal / (double) count);
        System.out.printf("Min Total Time: %d µs%n", minTotal);
        System.out.printf("Max Total Time: %d µs%n", maxTotal);
        System.out.printf("Effective wall-clock time: %.2f ms%n", effectiveWallClockMs);
    }

}
