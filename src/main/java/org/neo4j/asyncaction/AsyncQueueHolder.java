package org.neo4j.asyncaction;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.neo4j.asyncaction.AsyncQueueHolder.CreateRelationshipBean.POISON;

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
    public static final int INBOUND_QUEUE_SCAN_INTERVAL = 10;

    private final LogService logService;
    private final GraphDatabaseService graphDatabaseService;
    private final Log log;

    // contains CreateRelationshipBean instance with active transaction
    private BlockingQueue<CreateRelationshipBean> inboundQueue = new LinkedBlockingQueue<>(INBOUND_QUEUE_CAPACITY);

    // contains CreateRelationshipBean that are supposed to be processed, their originating transactions have been closed
    private BlockingQueue<CreateRelationshipBean> outboundQueue = new LinkedBlockingQueue<>(MAX_OPERATIONS_PER_TRANSACTION);

    public AsyncQueueHolder(GraphDatabaseService graphDatabaseService, LogService logService) {
        this.logService = logService;
        this.log = logService.getUserLog(AsyncQueueHolder.class);
        this.graphDatabaseService = graphDatabaseService;
    }

    public void add(Node startNode, Node endNode, String relationshipType, KernelTransaction kernelTransaction) {
        try {
            CreateRelationshipBean createRelationshipBean = new CreateRelationshipBean(startNode, endNode, relationshipType, kernelTransaction);
            log.debug("offering to queue " + createRelationshipBean);
            if (!inboundQueue.offer(createRelationshipBean, INBOUND_QUEUE_ADD_TIMEOUT, TimeUnit.MILLISECONDS)) {
                log.warn("timeout reached when adding to queue");
                inboundQueue.put(createRelationshipBean);
            };
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
                    CreateRelationshipBean command = outboundQueue.poll(100, TimeUnit.MILLISECONDS);
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
                            if (tx == null)  {
                                tx = graphDatabaseService.beginTx();
                                opsCount = 0;
                                timestamp = new Date().getTime();
                                log.info("new tx " + timestamp);
                            }
                            try {

                                command.getStartNode().createRelationshipTo(command.getEndNode(), RelationshipType.withName(command.getRelationshipType()));
                                opsCount++;
                                log.debug("processed " + command + " opscount=" + opsCount);
                            } catch (NotFoundException e) {
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

    private void startThreadForScanningInbound() {
        new Thread(() -> {
            try {
                int opsCount = 0;
                while (true) {
                    for (CreateRelationshipBean bean : inboundQueue) {
                        if (bean.equals(POISON)) {
                            log.info("got poison -> adding to outbound");
                            outboundQueue.put(POISON);
                            return;
                        }
                        if (!bean.getKernelTransaction().isOpen()) {
                            inboundQueue.remove(bean);
                            outboundQueue.put(bean);
                            opsCount++;
                            log.debug("moving " + bean + " to outbound " + opsCount);
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

    public static class CreateRelationshipBean {

        public static CreateRelationshipBean POISON = new CreateRelationshipBean(null, null, null, null);

        private final Node startNode;
        private final Node endNode;
        private final String relationshipType;
        private final KernelTransaction kernelTransaction;

        public CreateRelationshipBean(Node startNode, Node endNode, String relationshipType, KernelTransaction kernelTransaction) {
            this.startNode = startNode;
            this.endNode = endNode;
            this.relationshipType = relationshipType;
            this.kernelTransaction = kernelTransaction;
        }

        public Node getStartNode() {
            return startNode;
        }

        public Node getEndNode() {
            return endNode;
        }

        public String getRelationshipType() {
            return relationshipType;
        }

        public KernelTransaction getKernelTransaction() {
            return kernelTransaction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CreateRelationshipBean that = (CreateRelationshipBean) o;

            if (startNode != null ? !startNode.equals(that.startNode) : that.startNode != null) return false;
            if (endNode != null ? !endNode.equals(that.endNode) : that.endNode != null) return false;
            return relationshipType != null ? relationshipType.equals(that.relationshipType) : that.relationshipType == null;
        }

        @Override
        public int hashCode() {
            int result = startNode != null ? startNode.hashCode() : 0;
            result = 31 * result + (endNode != null ? endNode.hashCode() : 0);
            result = 31 * result + (relationshipType != null ? relationshipType.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            if (this.equals(POISON) ) {
                return "[POISON]";
            } else {
                return String.format("(%d)-[:%s]->(%d)", startNode.getId(), relationshipType, endNode.getId() );
            }
        }
    }
}
