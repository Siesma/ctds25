package database;

import query.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: <benchmark_dir> <num_threads>");
            System.exit(1);
        }

        String benchmarkDir = args[0];
        int numThreads = Integer.parseInt(args[1]);

        IDataManager dataManager = new DataManagerMVCC();
        ExecutionEngine engine = new ExecutionEngine(dataManager);
        TransactionManager tm = new TransactionManager(engine);
        Parser parser = new Parser();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            String filePath = String.format("%s/T%d", benchmarkDir, i + 1);
            Thread t = new Thread(() -> {
                List<Instruction> instructions = new ArrayList<>();

                try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        instructions.add(parser.parse(line));
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
    }
}
