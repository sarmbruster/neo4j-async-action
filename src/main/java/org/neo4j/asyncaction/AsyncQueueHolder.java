package org.neo4j.asyncaction;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.KernelTransactionsHelper;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    public static final GraphCommand POISON = (graphDatabaseService, log1) -> {};

    private final LogService logService;
    private final GraphDatabaseAPI graphDatabaseAPI;
    private final Log log;
    private final ThreadToStatementContextBridge threadToStatementContextBridge;

    // NB: KernelTransactions is not available in kernelExtension's dependencies - we need to use dependencyResolver in start()
    private KernelTransactions kernelTransactions;

    // contains GraphCommand instance with active transaction
    private BlockingQueue<Pair<GraphCommand, KernelTransactionHandle>> inboundQueue = new LinkedBlockingQueue<>(INBOUND_QUEUE_CAPACITY);

    // contains GraphCommand that are supposed to be processed, their originating transactions have been closed
    private BlockingQueue<GraphCommand> outboundQueue = new LinkedBlockingQueue<>(MAX_OPERATIONS_PER_TRANSACTION);

    public AsyncQueueHolder(GraphDatabaseAPI graphDatabaseAPI, LogService logService,
                            ThreadToStatementContextBridge threadToStatementContextBridge) {
        this.graphDatabaseAPI = graphDatabaseAPI;
        this.logService = logService;
        this.threadToStatementContextBridge = threadToStatementContextBridge;
        this.log = logService.getUserLog(AsyncQueueHolder.class);
    }

    public void add(GraphCommand command) {
        try {
            KernelTransaction kernelTransaction = threadToStatementContextBridge.getTopLevelTransactionBoundToThisThread(false);
            KernelTransactionHandle kernelTransactionHandle = KernelTransactionsHelper.getHandle(kernelTransactions, kernelTransaction);

            kernelTransaction.registerCloseListener(txId -> {
                inboundQueue.stream()
                        .filter(pair -> pair.other().equals(kernelTransactionHandle))
                        .forEach(pair -> {
                            GraphCommand cmd = pair.first();
                            inboundQueue.remove(pair);
                            outboundQueue.add(cmd);
                        });
                    }
            );
            log.debug("offering to queue %s" + command.toString());

            Pair<GraphCommand, KernelTransactionHandle> offering = Pair.of(
                    command,
                    KernelTransactionsHelper.getHandle(kernelTransactions, kernelTransaction)
            );
            if (!inboundQueue.offer(offering, INBOUND_QUEUE_ADD_TIMEOUT, TimeUnit.MILLISECONDS)) {
                log.warn("timeout reached when adding to queue");
                inboundQueue.put(offering);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() throws Throwable {
        kernelTransactions = graphDatabaseAPI.getDependencyResolver().resolveDependency(KernelTransactions.class);
        startThreadToProcessOutbound();
    }

    @Override
    public void stop() throws Throwable {

        while (!inboundQueue.isEmpty()) {
            Thread.sleep(10);
        }
        outboundQueue.put(POISON);
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
                            log.info("got poison -> terminating, opscount %d duration %d", opsCount, (now-timestamp));
                            tx.success();
                            tx.close();
                        }
                        return; // finish worker thread
                    } else {
                        if (tx!=null) {
                            long now = new Date().getTime();
                            if ((opsCount >= MAX_OPERATIONS_PER_TRANSACTION) || (now-timestamp > MAX_DURATION_PER_TRANSACTION)) { //either 1000 ops or 1000millis
                                log.info("rolling over transaction, opscount %d duration %d ", opsCount, (now-timestamp));
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
                if (tx!=null) {
                    tx.failure();
                    tx.close();
                }
                log.error("oops", e);
                throw new RuntimeException(e);
            }
        }).start();
    }


}
