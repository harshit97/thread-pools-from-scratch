package hello.concurrency.threadpool;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleFixedThreadPool {

    private static final AtomicInteger poolCount = new AtomicInteger(0);

    private final Queue<Runnable> runnableQueue;

    private final AtomicBoolean execute;

    private final List<SimpleThread> threadList;

    private SimpleFixedThreadPool(int threadCount) {
        int poolCountInt = poolCount.incrementAndGet();
        runnableQueue = new ConcurrentLinkedQueue<>(); // What is the difference between this and ConcurrentLinkedDeque?
        execute = new AtomicBoolean(true);
        threadList = new ArrayList<>();
        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            String threadName = MessageFormat.format("SimpleFixedThreadPool-{0}-SimpleThread-{1}", poolCountInt, threadIndex);
            SimpleThread simpleThread = new SimpleThread(threadName, execute, runnableQueue);
            this.threadList.add(simpleThread);
            simpleThread.start();
        }
    }

    /**
     * Gets a new SimpleFixedThreadPool instance with the number of threads specified
     *
     * @param threadCount Threads the pool must have
     * @return new SimpleFixedThreadPool
     */
    public static SimpleFixedThreadPool getInstance(int threadCount) {
        return new SimpleFixedThreadPool(threadCount);
    }

    /**
     * Gets a new SimpleFixedThreadPool instance with number of threads equal to the number of processors (or CPU threads) available
     *
     * @return new SimpleFixedThreadPool
     */
    public static SimpleFixedThreadPool getInstance() {
        return getInstance(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Adds a runnable to the queue for execution
     *
     * @param runnable Runnable to be added to the pool
     */
    public void execute(Runnable runnable) {
        if (execute.get()) {
            runnableQueue.offer(runnable);
        } else {
            throw new IllegalStateException("ThreadPool is terminating, unable to execute runnable.");
        }
    }

    /**
     * Clears the queue of runnables and stops the threadpool. Any runnables currently executing will continue to execute.
     */
    public void terminate() {
        runnableQueue.clear();
        stop();
    }

    /**
     * Stops addition of new runnables to the threadpool and terminates the pool once all runnables in the queue are executed.
     */
    public void stop() {
        execute.set(false);
    }

    /**
     * Awaits the termination of the threads in the threadpool indefinitely
     *
     * @throws IllegalStateException Thrown if the stop() or terminate() methods haven't been called before awaiting
     */
    public void awaitTermination() throws IllegalStateException {
        if (execute.get()) {
            throw new IllegalStateException("ThreadPool not termicated");
        }

        while (true) {
            boolean areThreadsAlive = false;

            for (SimpleThread thread : this.threadList) {
                if (thread.isAlive()) {
                    areThreadsAlive = true;
                    break;
                }
            }

            if (!areThreadsAlive) {
                return;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new SimpleThreadPoolException(e);
            }
        }
    }

    /**
     * Awaits up to <b>timeout</b> ms the termination of the threads in the threadpool
     *
     * @param timeoutInMillis Timeout in milliseconds
     * @throws TimeoutException      Thrown if the termination takes longer than the timeout
     * @throws IllegalStateException Thrown if the stop() or terminate() methods haven't been called before awaiting
     */
    public void awaitTermination(long timeoutInMillis) throws TimeoutException, IllegalStateException {
        long startTime = System.currentTimeMillis();
        if (execute.get()) {
            throw new IllegalStateException("ThreadPool not terminated");
        }

        while ((System.currentTimeMillis() - startTime) <= timeoutInMillis) {
            boolean areThreadsAlive = false;

            for (SimpleThread thread : this.threadList) {
                if (thread.isAlive()) {
                    areThreadsAlive = true;
                    break;
                }
            }

            if (!areThreadsAlive) {
                return;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new SimpleThreadPoolException(e);
            }
        }
        throw new TimeoutException("Unable to terminate threadpool within the specified timeout (" + timeoutInMillis + "ms)");
    }

    /**
     * Inner Thread class which represents the threads in the pool. It acts as a wrapper for all runnables in the queue.
     */
    private class SimpleThread extends Thread {

        private final AtomicBoolean execute;
        private final Queue<Runnable> runnableQueue;

        SimpleThread(String name, AtomicBoolean execute, Queue<Runnable> runnableQueue) {
            super(name);
            this.execute = execute;
            this.runnableQueue = runnableQueue;
        }

        @Override
        public void run() {
            try {
                while (!execute.get() || !runnableQueue.isEmpty()) {
                    Runnable runnable;
                    while ((runnable = runnableQueue.poll()) != null) {
                        runnable.run();
                    }
                    Thread.sleep(1);
                }
            } catch (RuntimeException | InterruptedException e) {
                throw new SimpleThreadPoolException(e);
            }
        }

    }

    /**
     * Thrown when there's a RuntimeException or InterruptedException when executing a runnable from the queue,
     * or awaiting termination
     */
    class SimpleThreadPoolException extends RuntimeException {
        SimpleThreadPoolException(Throwable cause) {
            super(cause);
        }
    }
}
