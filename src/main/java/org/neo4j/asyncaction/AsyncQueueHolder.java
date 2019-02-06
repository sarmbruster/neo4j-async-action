package org.neo4j.asyncaction;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.KernelTransactionsHelper;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.*;

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

    private final LogService logService;

    private final GraphDatabaseAPI graphDatabaseAPI;

    private final Log log;

    private final ThreadToStatementContextBridge threadToStatementContextBridge;

    // NB: KernelTransactions is not available in kernelExtension's dependencies - we need to use dependencyResolver in start()
    private KernelTransactions kernelTransactions;

    // contains GraphCommand instance with active transaction
    private ConcurrentMap<Long, Collection<GraphCommand>> inboundGraphCommandsMap = new ConcurrentHashMap<>();

    // contains GraphCommand that are supposed to be processed, their originating transactions have been closed
    private BlockingQueue<GraphCommand> outboundQueue = new LinkedBlockingQueue<>(MAX_OPERATIONS_PER_TRANSACTION);

    // set temporarily to true while closing queue
    private boolean isClosing = false;

    // prevent multiple invocations of stop()
    private boolean closed = false;

    public AsyncQueueHolder(GraphDatabaseAPI graphDatabaseAPI, LogService logService,
                            ThreadToStatementContextBridge threadToStatementContextBridge) {

        this.graphDatabaseAPI = graphDatabaseAPI;
        this.logService = logService;
        this.threadToStatementContextBridge = threadToStatementContextBridge;
        this.log = logService.getUserLog(AsyncQueueHolder.class);
    }

    public void add(GraphCommand command) {
        KernelTransaction kernelTransaction = threadToStatementContextBridge.getTopLevelTransactionBoundToThisThread(false);
        KernelTransactionHandle kernelTransactionHandle = KernelTransactionsHelper.getHandle(kernelTransactions, kernelTransaction);
        long transactionId = kernelTransactionHandle.getUserTransactionId();

        Collection<GraphCommand> graphCommands = inboundGraphCommandsMap.computeIfAbsent(transactionId, (id) -> new ArrayList<>());
        if (graphCommands.isEmpty()) {
            log.debug("registering close listener for tx %s", kernelTransactionHandle.getUserTransactionName());
            kernelTransaction.registerCloseListener(txId -> {
                        Collection<GraphCommand> commands = inboundGraphCommandsMap.remove(transactionId);
                        if ((commands != null) && (txId != org.neo4j.internal.kernel.api.Transaction.ROLLBACK)) {
                            outboundQueue.addAll(commands);
                        }
                    }
            );
        }
        graphCommands.add(command);
        log.debug("added command to inbound queue %s", command.toString());
    }

    @Override
    public void start() {
        kernelTransactions = graphDatabaseAPI.getDependencyResolver().resolveDependency(KernelTransactions.class);
        startThreadToProcessOutbound();
    }

    @Override
    public void stop() {
        if (!closed) {
            try {
                while (!inboundGraphCommandsMap.isEmpty()) {
                    Thread.sleep(10);
                }

                isClosing = true;
                outboundQueue.put(POISON);

                while (isClosing) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            closed = true;
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
}
