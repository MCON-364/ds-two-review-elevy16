package edu.touro.mcon364.finalreview.orderflowhandoff.homework;

import edu.touro.mcon364.finalreview.model.LogMessage;
import edu.touro.mcon364.finalreview.model.SensorReading;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Homework 2 — Sensor reading processor.
 * <p>
 * A monitoring system receives readings from sensors over time. One part of the
 * program submits readings as they arrive. Another part of the program processes
 * those readings using one or more background workers.
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
    // pending work
    private final BlockingQueue<SensorReading> queue = new LinkedBlockingQueue<>();

    // worker threads
    private final List<Thread> workers = new ArrayList<>();

    // is the processor still running?
    private volatile boolean running = false;

    // total messages processed
    private final AtomicInteger totalProcessed = new AtomicInteger(0);

    // count by log level
    private final DoubleSummaryStatistics stats = new DoubleSummaryStatistics();

    /**
     * Accept one sensor reading for processing.
     *
     * @param reading the reading to process later
     */
    public void submit(SensorReading reading) {
        // TODO: decide where submitted readings should be stored
        try {
            queue.put(reading);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
        // TODO: start the requested number of workers
        for (int i = 0; i < workerCount; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
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
                SensorReading reading = queue.poll(100, TimeUnit.MILLISECONDS);
                if (reading != null) {
                    process(reading);
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
private void process(SensorReading message) {
    // TODO: implement
    totalProcessed.incrementAndGet();
    synchronized (stats) {
        stats.accept(message.value());
    }
}

/**
 * Stop the processor and wait for workers to finish.
 *
 * @throws InterruptedException if the calling thread is interrupted while waiting
 */
public void stop() throws InterruptedException {
    // TODO: signal that work should stop
    // TODO: wait for all workers to finish
}

/**
 * Return the number of readings processed so far.
 */
public int getTotalProcessed() {
    // TODO: return the processed count safely
    return 0;
}

/**
 * Return summary statistics for the processed reading values.
 * <p>
 * If no readings have been processed yet, return an empty
 * DoubleSummaryStatistics object.
 */
public DoubleSummaryStatistics getStats() {
    // TODO: calculate or return the current statistics safely
    return new DoubleSummaryStatistics();
}
}
