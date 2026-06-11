package edu.touro.mcon364.finalreview.orderflowhandoff.homework;

import edu.touro.mcon364.finalreview.model.LogMessage;
import edu.touro.mcon364.finalreview.model.SensorReading;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Homework 2 — Sensor reading processor.
 * <p>
 * A monitoring system receives readings from sensors over time. One part of the
 * program submits readings as they arrive. Another part of the program processes
 * those readings using one or more background workers. (so executor service)
 * <p>
 * This class is responsible for coordinating that handoff and for keeping a
 * summary of the readings that were actually processed.
 * <p>
 * The important question is not only "How do we calculate the stats?" It is also:
 * "What happens when readings are being submitted and processed by different
 * threads at the same time?"
 * <p>
 * Requirements:
 * - submit(reading) accepts one new sensor reading for later processing.
 * - start(workerCount) starts workerCount background workers.
 * - workerCount must be greater than 0.
 * - Workers should process submitted readings until the processor is stopped and
 * all already-submitted readings have been handled.
 * - stop() tells the processor to stop accepting/processing future work and waits
 * until the workers finish the remaining work.
 * - getTotalProcessed() returns how many readings have been processed so far.
 * - getStats() returns summary statistics for the processed reading values:
 * count, minimum, maximum, sum, and average.
 * - Public reporting methods must not expose mutable internal state.
 * <p>
 * Before coding, think about:
 * - Which object or objects represent work waiting to be processed?
 * - Which object or objects represent work that has already been processed?
 * - Which state can be accessed by more than one thread?
 * - How will workers know when to keep working and when to stop?
 * - What should happen if getStats() is called while workers are still running?
 * - Is it better to store all processed readings and calculate stats later, or
 * update numeric summary state as each reading is processed?
 * - If several workers update the same stats, how will those updates stay correct?
 */


public class SensorProcessor {

    // fields
    // pending work - thread safe queue which is a blockingQueue
    private final BlockingQueue<SensorReading> queue = new LinkedBlockingQueue<>();

    // worker threads
    private ExecutorService executor;

    // is the processor still running?
    // best approach to initialize this to false and then start changes it to true
    private volatile boolean running = false;

    // total messages processed
    private final AtomicInteger totalProcessed = new AtomicInteger(0);

    // count by log level - summary statistics can use atomic reference or lock
    private final DoubleSummaryStatistics stats = new DoubleSummaryStatistics();

    /**
     * Accept one sensor reading for processing.
     *
     * @param reading the reading to process later
     */
    public void submit(SensorReading reading) {
        // TODO: decide where submitted readings should be stored
        if (running) {
            queue.offer(reading);
        }

    }

    /**
     * Start background workers that process submitted readings.
     *
     * @param workerCount number of worker threads to start
     * @throws IllegalArgumentException if workerCount is not positive
     */
    public void start(int workerCount) {
        // TODO: validate workerCount
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be greater than 0");
        }
        running = true;
        executor = Executors.newFixedThreadPool(workerCount);
        // TODO: start the requested number of workers
        for (int i = 0; i < workerCount; i++) {
            executor.submit(this::workerLoop);
        }
    }

    /**
     * Logic run by each worker.
     * <p>
     * This method is private because callers should not run worker logic directly.
     * The worker should repeatedly look for work, process it when available, and
     * eventually exit when the processor is stopping and no work remains.
     */
    private void workerLoop() {
        // TODO: implement the worker behavior
        while (running || !queue.isEmpty()) {
            try {
                SensorReading reading = queue.poll(100, TimeUnit.MILLISECONDS); // can use take, or poll with a timeout
                if (reading != null) {
                    // process the reading using lock (can also use atomic reference)
                    totalProcessed.incrementAndGet();
                    synchronized (stats) {
                        stats.accept(reading.value());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }


/**
 * Process one message and update whatever statistics this class tracks.
 */
/*
// do this method only if use atomic reference instead of just locking
private void process(SensorReading message) {
    // TODO: implement
    totalProcessed.incrementAndGet();
    stats.updateAndGet(existing -> {
        DoubleSummaryStatistics updated = new DoubleSummaryStatistics();
        // combine the existing stats with the new reading
        updated.combine(existing);
        // update the stats with new reading value
        updated.accept(reading.value());
        return updated;
    });
}
 */

    /**
     * Stop the processor and wait for workers to finish.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void stop() throws InterruptedException {
        // TODO: signal that work should stop
        running = false;
        // check that executor is not null before trying to shut it down
        // if its null, that means start() was never called, and there are no workers to shutdown
        if (executor != null) {
            executor.shutdown();

            // TODO: wait for all workers to finish
            // we are not told how long to wait, so we will waiat indefinitely until they finish
            executor.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
        /*
        while (!queue.isEmpty()) {
            process(queue.poll());

            }

         */
    }


    /**
     * Return the number of readings processed so far.
     */
    public int getTotalProcessed() {
        // TODO: return the processed count safely
        return totalProcessed.get();
    }

    /**
     * Return summary statistics for the processed reading values.
     * <p>
     * If no readings have been processed yet, return an empty
     * DoubleSummaryStatistics object.
     */
    public DoubleSummaryStatistics getStats() {
        // TODO: calculate or return the current statistics safely
        synchronized (stats) {
            // create a new object and combine with current stats
            DoubleSummaryStatistics copy = new DoubleSummaryStatistics();
            copy.combine(stats);
            return copy;
        }
    }
}
