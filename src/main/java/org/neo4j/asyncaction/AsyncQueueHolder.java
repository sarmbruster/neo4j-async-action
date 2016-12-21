package org.neo4j.asyncaction;

import org.neo4j.asyncaction.command.GraphCommand;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.neo4j.asyncaction.command.PoisonCommand.POISON;

/**
 * @author Stefan Armbruster
 */
public class AsyncQueueHolder extends LifecycleAdapter {

    /**
     * roll over transaction latest at this number of operations
     */
    public static final int MAX_OPERATIONS_PER_TRANSACTION = 10000;

    /**
     * roll over transaction latest after this duration in milliseconds
     */
    public static final int MAX_DURATION_PER_TRANSACTION = 1000; // millis

    /**
     * timeout for adding to inbound queue. A warning is spit out if this timeout is reached and afterwards we block.
     */
    public static final int INBOUND_QUEUE_ADD_TIMEOUT = 100;


    public static final int INBOUND_QUEUE_CAPACITY = 100000;

    /**
     * scan inbound queue for finished transactions every x milliseconds
     */
    public static final int INBOUND_QUEUE_SCAN_INTERVAL = 100;

    private final LogService logService;
    private final GraphDatabaseService graphDatabaseService;
    private final Log log;

    // contains GraphCommand instance with active transaction
    private BlockingQueue<GraphCommand> inboundQueue = new LinkedBlockingQueue<>(INBOUND_QUEUE_CAPACITY);

    // contains GraphCommand that are supposed to be processed, their originating transactions have been closed
    private BlockingQueue<GraphCommand> outboundQueue = new LinkedBlockingQueue<>(MAX_OPERATIONS_PER_TRANSACTION);

    public AsyncQueueHolder(GraphDatabaseService graphDatabaseService, LogService logService) {
        this.logService = logService;
        this.log = logService.getUserLog(AsyncQueueHolder.class);
        this.graphDatabaseService = graphDatabaseService;
    }

    public void add(GraphCommand command) {
        try {
            log.debug("offering to queue " + command);
            if (!inboundQueue.offer(command, INBOUND_QUEUE_ADD_TIMEOUT, TimeUnit.MILLISECONDS)) {
                log.warn("timeout reached when adding to queue");
                inboundQueue.put(command);
            }
            ;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() throws Throwable {
        startThreadForScanningInbound();
        startThreadToProcessOutbound();
    }

    private void startThreadToProcessOutbound() {
        new Thread(() -> {
            Transaction tx = null;
            int opsCount = 0;
            long timestamp = -1;

            try {
                while (true) {
                    GraphCommand command = outboundQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (POISON.equals(command)) {
                        if (tx!=null) {
                            long now = new Date().getTime();
                            log.info("got poison -> terminating, opscount " + opsCount + " duration " + (now-timestamp));
                            tx.success();
                            tx.close();
                        }
                        return; // finish worker thread
                    } else {
                        if (tx!=null) {
                            long now = new Date().getTime();
                            if ((opsCount >= MAX_OPERATIONS_PER_TRANSACTION) || (now-timestamp > MAX_DURATION_PER_TRANSACTION)) { //either 1000 ops or 1000millis
                                log.info("rolling over transaction, opscount " + opsCount + " duration " + (now-timestamp));
                                tx.success();
                                tx.close();
                                tx = null;
                            }
                        }

                        if (command != null) {
                            if (tx == null) {
                                tx = graphDatabaseService.beginTx();
                                opsCount = 0;
                                timestamp = new Date().getTime();
                                log.info("new tx " + timestamp);
                            }
                            command.run(graphDatabaseService, log);
                            opsCount++;
                            log.debug("processed " + command + " opscount=" + opsCount);
                        }
                    }
                }
            } catch (Exception e) {
                if (tx!=null) {
                    tx.failure();
                    tx.close();
                }
                log.error("oops", e);
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void startThreadForScanningInbound() {
        new Thread(() -> {
            try {
                int opsCount = 0;
                while (true) {
                    for (GraphCommand command : inboundQueue) {
                        if (command.equals(POISON)) {
                            log.info("got poison -> adding to outbound");
                            outboundQueue.put(POISON);
                            return;
                        }
                        if (!command.getKernelTransaction().isOpen()) {
                            inboundQueue.remove(command);
                            outboundQueue.put(command);
                            opsCount++;
                            log.debug("moving " + command + " to outbound " + opsCount);
                        }
                    }
//                    log.info("inbound: opscount " + opsCount + " size " + inboundQueue.size());
                    Thread.sleep(INBOUND_QUEUE_SCAN_INTERVAL);
                }
            } catch (Exception e) {
                log.error("oops", e);
                throw new RuntimeException(e);
            }
        }).start();
    }

    @Override
    public void stop() throws Throwable {
        inboundQueue.put(POISON);
    }

}
