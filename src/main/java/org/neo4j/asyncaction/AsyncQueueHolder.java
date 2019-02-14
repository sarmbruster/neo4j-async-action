package org.neo4j.asyncaction;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;

public class AsyncQueueHolder extends LifecycleAdapter {

    /**
     * roll over transaction latest at this number of operations
     */
    public static final int MAX_OPERATIONS_PER_TRANSACTION = 10000;

    /**
     * roll over transaction latest after this duration in milliseconds
     */
    public static final int MAX_DURATION_PER_TRANSACTION = 1000; // millis

    public static final GraphCommand POISON = (graphDatabaseService, log1) -> {
    };

    private final GraphDatabaseAPI graphDatabaseAPI;

    private final Log log;

    private final ThreadToStatementContextBridge threadToStatementContextBridge;

    // contains GraphCommand instances per each active transaction
    private ConcurrentMap<KernelTransaction, List<GraphCommand>> inboundGraphCommandsMap = new ConcurrentHashMap<>();

    // contains GraphCommand that are supposed to be processed, their originating transactions have been closed
    private BlockingQueue<GraphCommand> outboundQueue = new LinkedBlockingQueue<>(MAX_OPERATIONS_PER_TRANSACTION);

    // set temporarily to true while closing queue
    private boolean isClosing = false;

    // prevent multiple invocations of stop()
    private boolean closed = false;

    public AsyncQueueHolder(GraphDatabaseAPI graphDatabaseAPI, LogService logService,
                            ThreadToStatementContextBridge threadToStatementContextBridge) {
        this.graphDatabaseAPI = graphDatabaseAPI;
        this.threadToStatementContextBridge = threadToStatementContextBridge;
        this.log = logService.getUserLog(AsyncQueueHolder.class);
    }

    public void add(GraphCommand command) {
        KernelTransaction kernelTransaction = threadToStatementContextBridge.getTopLevelTransactionBoundToThisThread(false);
        Collection<GraphCommand> graphCommands = inboundGraphCommandsMap.computeIfAbsent(kernelTransaction, (id) -> new ArrayList<>());
        if (graphCommands.isEmpty()) {
            log.debug("registering close listener for tx %s", kernelTransaction.toString());
            // we need to use a closeListener here to also get notified in case of rollback
            // normal txEventHandlers are not sufficient since they don't get called upon a read only tx.
            kernelTransaction.registerCloseListener(txId -> {
                        Collection<GraphCommand> commands = inboundGraphCommandsMap.remove(kernelTransaction);
                        if ((commands != null) && (txId != -1)) {
                            commands.forEach(cmd -> {
                                try {
                                    outboundQueue.put(cmd);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                        }
                    }
            );
        }
        graphCommands.add(command);
        log.debug("added command to inbound queue %s", command.toString());
    }

    @Override
    public void start() {
        startThreadToProcessOutbound();
    }

    @Override
    public void stop() {
        if (!closed) {
            try {
                waitWhile(aVoid -> !inboundGraphCommandsMap.isEmpty());
                isClosing = true;
                outboundQueue.put(POISON);
                waitWhile(aVoid -> isClosing);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            closed = true;
        }
    }

    private void waitWhile(Predicate<Void> predicate) throws InterruptedException {
        while (predicate.test(null)) {
            Thread.sleep(10);
        }
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
                        if (tx != null) {
                            long now = new Date().getTime();
                            log.info("got poison -> terminating, opscount %d duration %d", opsCount, (now - timestamp));
                            tx.success();
                            tx.close();
                        }
                        isClosing = false;
                        return; // finish worker thread
                    } else {
                        if (tx != null) {
                            long now = new Date().getTime();
                            if ((opsCount >= MAX_OPERATIONS_PER_TRANSACTION) || (now - timestamp > MAX_DURATION_PER_TRANSACTION)) { //either 1000 ops or 1000millis
                                log.info("rolling over transaction, opscount %d duration %d ", opsCount, (now - timestamp));
                                tx.success();
                                tx.close();
                                tx = null;
                            }
                        }

                        if (command != null) {
                            if (tx == null) {
                                tx = graphDatabaseAPI.beginTx();
                                opsCount = 0;
                                timestamp = new Date().getTime();
                                log.debug("new tx %d", timestamp);
                            }
                            try {
                                command.accept(graphDatabaseAPI, log);
                                opsCount++;
                                log.debug("processed %s opscount=%d", command.toString(), opsCount);
                            } catch (RuntimeException e) {
                                log.error("oops", e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (tx != null) {
                    tx.failure();
                    tx.close();
                }
                log.error("oops", e);
                throw new RuntimeException(e);
            }
        }).start();
    }

    public boolean isQueueEmpty() {
        return outboundQueue.isEmpty();
    }

    public boolean isInBoundEmpty() {
        return inboundGraphCommandsMap.isEmpty();
    }
}
