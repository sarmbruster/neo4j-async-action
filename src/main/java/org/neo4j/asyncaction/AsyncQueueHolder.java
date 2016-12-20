package org.neo4j.asyncaction;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
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

    private final LogService logService;
    private final GraphDatabaseService graphDatabaseService;

    // contains CreateRelationshipBean instance with active transaction
    private BlockingQueue<CreateRelationshipBean> inboundQueue = new LinkedBlockingQueue<>(5);

    // contains CreateRelationshipBean that are supposed to be processed, their originating transactions have been closed
    private BlockingQueue<CreateRelationshipBean> outboundQueue = new LinkedBlockingQueue<>(5);

    private Log log;

    public AsyncQueueHolder(GraphDatabaseService graphDatabaseService, LogService logService) {
        this.logService = logService;
        this.log = logService.getUserLog(AsyncQueueHolder.class);
        this.graphDatabaseService = graphDatabaseService;
    }

    public void add(Node startNode, Node endNode, String relationshipType, KernelTransaction kernelTransaction) {
        try {
            CreateRelationshipBean createRelationshipBean = new CreateRelationshipBean(startNode, endNode, relationshipType, kernelTransaction);
            log.info("offering to queue " + createRelationshipBean);
            System.out.println("offering to queue " + createRelationshipBean);
            if (!inboundQueue.offer(createRelationshipBean, 100, TimeUnit.MILLISECONDS)) {
                System.out.println("timeout reached when adding to queue");
                log.error("timeout reached when adding to queue");
            };
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void start() throws Throwable {
        new Thread(() -> {
            try {
                while (true) {
                    for (CreateRelationshipBean bean : inboundQueue) {
                        if (bean.equals(POISON)) {
                            return;
                        }
                        if (!bean.getKernelTransaction().isOpen()) {
                            inboundQueue.remove(bean);
                            outboundQueue.put(bean);
                        }
                    }
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                System.out.println("oops" + e);
                log.error("oops", e);
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                Transaction tx = null;
                int opsCount = 0;
                long timestamp = -1;

                while (true) {
                    CreateRelationshipBean command = outboundQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (POISON.equals(command)) {
                        System.out.println("got poison -> terminating");
                        tx.success();
                        tx.close();
                        return; // finish worker thread
                    } else {
                        if (tx!=null) {
                            long now = new Date().getTime();
                            if ((opsCount > 1000) || (now-timestamp > 1000)) { //either 1000 ops or 1000millis
                                System.out.printf("rolling over transaction " );
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
                                System.out.printf("new tx " + timestamp);
                            }
                            command.getStartNode().createRelationshipTo(command.getEndNode(), RelationshipType.withName(command.getRelationshipType()));
                            opsCount++;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("oops " + e);
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
