package edu.touro.mcon364.finalreview.orderflowhandoff.exercises;

import edu.touro.mcon364.finalreview.model.LogLevel;
import edu.touro.mcon364.finalreview.model.LogMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LogProcessor.
 * <p>
 * A server receives log messages from different parts of an application:
 * authentication, payments, reporting, background jobs, and so on. Messages may
 * arrive while earlier messages are still being processed. We want one part of
 * the program to submit log messages, and a small group of worker threads to
 * process those messages in the background.
 * <p>
 * This class represents that log-processing service.
 * <p>
 * The main problem you are solving:
 * - incoming messages need to wait somewhere until a worker is ready for them;
 * - more than one worker may be running at the same time;
 * - every submitted message should be processed once;
 * - while messages are processed, the class must keep accurate summary counts.
 * <p>
 * Requirements:
 * - submit(message) accepts one log message for later processing.
 * - start(workerCount) starts exactly workerCount background workers.
 * - workerCount must be positive.
 * - workers should keep processing while the processor is still accepting work
 * or while there is still unprocessed work waiting.
 * - stop() tells the processor to stop accepting/expecting more work and waits
 * until the already-submitted work has been handled.
 * - getTotalProcessed() returns how many log messages have been processed.
 * - getCountsByLevel() returns how many processed messages there were for each
 * LogLevel.
 * - getCountsByLevel() must not allow callers to mutate this class's internal
 * state.
 * - The class must behave correctly when multiple threads interact with it.
 * <p>
 * Questions to think about before coding:
 * - Where should submitted messages wait before a worker processes them?
 * - What behavior do we need from that structure: newest first, oldest first,
 * priority order, or something else?
 * - Which state is shared by multiple threads?
 * - Which operations must be protected so the statistics stay correct?
 * - How will worker threads know when to continue waiting for work and when to
 * finish?
 * - What should happen if stop() is called while messages are still waiting?
 * - What should the public getter methods return so outside code cannot damage
 * the processor's internal state?
 */
public class LogProcessor {

    /*
     * Decide what fields this class needs.
     *
     * Think about:
     * - pending work
     * - worker threads
     * - whether the processor is still running
     * - total processed count
     * - count by log level
     */
    // pending work
    private final BlockingQueue<LogMessage> queue = new LinkedBlockingQueue<>();

    // worker threads
    private ExecutorService executor;

    // is the processor still running?
    private volatile boolean running = false;

    // total messages processed
    private final AtomicInteger totalProcessed = new AtomicInteger(0);

    // count by log level
    private final ConcurrentHashMap<LogLevel, Integer> countsByLevel = new ConcurrentHashMap<>();
    // can use AtomicInteger instead of Integer but more of a hassle

    /**
     * Accept one message for processing.
     */
    public void submit(LogMessage message) {
        // TODO: implement
        if (running) {
            queue.offer(message);
        }
    }

    /**
     * Start the requested number of background workers.
     */
    public void start(int workerCount) {
        // TODO: implement
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be greater than 0");
        }
        running = true;
        executor = Executors.newFixedThreadPool(workerCount);
        for (int i = 0; i < workerCount; i++) {
            executor.submit(this::workerLoop);
        }
    }

    /**
     * The work done by one background worker.
     * <p>
     * You may keep this helper method, rename it, or replace it with another
     * private helper if your design is clearer that way.
     */
    private void workerLoop() {
        // TODO: implement
        try {
            while (running || !queue.isEmpty()) {
                process(queue.take());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Process one message and update whatever statistics this class tracks.
     */
    private void process(LogMessage message) {
        // TODO: implement
        totalProcessed.incrementAndGet();
        countsByLevel.merge(message.level(), 1, Integer::sum);
        // merge takes key, value, and method - add value to key using the method
    }

    /**
     * Stop the processor and wait for worker threads to finish.
     */
    public void stop() throws InterruptedException {
        // TODO: implement
        running = false;
        if (executor == null) return;
        executor.shutdownNow(); // interrupt workers blocked on take()
        // blocks main thread until all workers have finished, or the timeout occurs, or the thread is interrupted
        executor.awaitTermination(1, TimeUnit.SECONDS);
        // drain any messages that were in the queue but not yet picked up
        LogMessage msg;
        while ((msg = queue.poll()) != null) {
            process(msg);
        }

    }

    /**
     * Return the number of messages processed so far.
     */
    public int getTotalProcessed() {
        // TODO: implement
        return totalProcessed.get();
    }

    /**
     * Return a safe snapshot of the counts by level.
     */
    public Map<LogLevel, Integer> getCountsByLevel() {
        // TODO: implement
        return Map.copyOf(countsByLevel);
    }
}
