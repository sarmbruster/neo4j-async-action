package org.neo4j.asyncaction;

import org.junit.*;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProceduresTests {

    private GraphDatabaseAPI db;
    private String boltUrl;

    @Before
    public void startDatabase() throws KernelException {
        BoltConnector connector = new BoltConnector("bolt");
        int port = PortAuthority.allocatePort();
        boltUrl = "bolt://localhost:" + port;
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setUserLogProvider(new AssertableLogProvider(true))
                .newImpermanentDatabaseBuilder()
                .setConfig(connector.listen_address, ListenSocketAddress.listenAddress("0.0.0.0", port))
                .setConfig(connector.enabled, "true")
                .setConfig("async.queueSize", "5") // use tiny queue for tests to make sure we don't fail
                .newGraphDatabase();
        registerProcedure(db, Procedures.class);
    }
    
    @After
    public void stopDatabase() {
        finishQueueAndWait();        
        db.shutdown();
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?>...procedures) throws KernelException {
        org.neo4j.kernel.impl.proc.Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(org.neo4j.kernel.impl.proc.Procedures.class);
        for (Class<?> procedure : procedures) {
            proceduresService.registerProcedure(procedure,true);
            proceduresService.registerFunction(procedure, true);
            proceduresService.registerAggregationFunction(procedure, true);
        }
    }

    @Test
    public void verifyNoDeadlock() {

        final long languageNodeId = Iterators.single(db.execute("CREATE (a:LANGUAGE) RETURN id(a) as id").columnAs("id"));

        List<Long> patentIds = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 100; i++) {
                final Long nodeId = db.createNode().getId();
                patentIds.add(nodeId);
            }
            tx.success();
        }

        try (final Driver driver = GraphDatabase.driver(boltUrl)) {
            patentIds.parallelStream().forEach((patentId) -> {

                try (Session session = driver.session(AccessMode.WRITE)) {
                    session.writeTransaction( tx ->
                         tx.run("CALL async.createRelationshipByIds($languageId, $patentId, 'HAS')",
                                MapUtil.map(
                                        "languageId", languageNodeId,
                                        "patentId", patentId
                                )
                        )

                    );
                }
            });
        }

        finishQueueAndWait();

        long count = Iterators.single(db.execute("MATCH ()-[r:HAS]->() RETURN count(r) AS count").columnAs("count"));
        Assert.assertEquals(100, count);
    }

    @Test
    public void verifyPropertiesAreAddedTest() {

        db.execute("CREATE (a), (b) WITH a,b UNWIND range(1,20) AS x CALL async.createRelationship(a, b, 'KNOWS', {test : 2}) RETURN a,b");
        finishQueueAndWait();

        for (Map<String, Object> result : Iterators.asIterable(db.execute("MATCH ()-[r:KNOWS]->() RETURN r"))) {

            final Transaction transaction = db.beginTx();

            Assert.assertEquals(1, result.keySet().size());
            Assert.assertEquals(1, ((Relationship) result.get("r")).getAllProperties().keySet().size());
            Assert.assertEquals(2L, ((Relationship) result.get("r")).getProperty("test"));
            transaction.success();
            transaction.close();

        }
    }

    @Test
    public void shouldAsyncCreationOfRelationshipsWork() {

        db.execute("CREATE (a), (b) WITH a,b UNWIND range(1,20) AS x CALL async.createRelationship(a, b, 'KNOWS', {}) RETURN a,b");
        finishQueueAndWait();

        long count = Iterators.single(db.execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count").columnAs("count"));
        assertEquals(20, count);

    }

    @Test
    public void shouldAsyncCreationOfRelationshipViaCypherWork() {

        db.execute("CREATE (a), (b) \n" +
                "WITH a,b\n" +
                "UNWIND range(1,20) as x\n" +
                "CALL async.cypher(\"with $start as start, $end as end create (start)-[:KNOWS]->(end)\", {start:a, end:b})\n" +
                "RETURN a,b");
        finishQueueAndWait();

        long count = Iterators.single(db.execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count").columnAs("count"));
        assertEquals(20, count);
    }

    @Test
    public void shouldConnectingToDenseNodeWorkWithCypher() {

        db.execute("CREATE INDEX ON :Person(id)");
        db.execute("MERGE (dense:Person{id:'dense'})");

        boolean regularTermination = runWithExecutorService(1000, o ->
                db.execute("MERGE (dense:Person{id:'dense'}) \n" +
                "MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})\n" +
                "MERGE (rnd)-[:KNOWS]->(dense)"));

        assertTrue(regularTermination);

        long count = Iterators.single(db.execute("MATCH (p:Person) RETURN count(p) AS c").columnAs("c"));
        assertTrue(count < 1000);

        count = Iterators.single(db.execute("MATCH (p:Person{id:'dense'}) RETURN size((p)<-[:KNOWS]-()) AS c").columnAs("c"));
        assertTrue(count < 1000);
    }

    @Test
    public void testConnectingToDenseNodeUsingAsync() {

        db.execute("CREATE INDEX ON :Person(id)");
        db.execute("MERGE (dense:Person{id:'dense'})");
        boolean regularTermination = runWithExecutorService(1000, index -> db.execute("MERGE (dense:Person{id:'dense'}) \n" +
                "    CREATE (rnd:Person{id:$id})\n" +
                "    WITH dense, rnd \n" +
                "    CALL async.createRelationship(rnd, dense, 'KNOWS', {}) \n" +
                "    RETURN dense, rnd", Collections.singletonMap("id", "person_" + index)));
        assertTrue(regularTermination);

        long count = Iterators.single(db.execute("MATCH (p:Person) RETURN count(p) AS c").columnAs("c"));
        assertEquals(1001, count);

        count = Iterators.single(db.execute("MATCH (p:Person{id:'dense'}) RETURN size((p)<-[:KNOWS]-()) AS c").columnAs("c"));
        assertEquals(1000, count);
    }

    @Test
    public void testConnectingToDenseNodeUsingAsyncMerge() {

        db.execute("CREATE INDEX ON :Person(id)");
        db.execute("MERGE (dense:Person{id:'dense'})");
        boolean regularTermination = runWithExecutorService(1000, index -> db.execute("MERGE (dense:Person{id:'dense'}) \n" +
                "    MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})\n" +
                "    WITH dense, rnd \n" +
                "    CALL async.mergeRelationship(rnd, dense, 'KNOWS') \n" +
                "    RETURN dense, rnd"));

        assertTrue(regularTermination);

        long count = Iterators.single(db.execute("MATCH (p:Person) RETURN count(p) AS c").columnAs("c"));
        assertTrue(count < 800);

        count = Iterators.single(db.execute("MATCH (p:Person{id:'dense'}) RETURN size((p)<-[:KNOWS]-()) AS c").columnAs("c"));
        assertTrue(count < 800);

    }

    @Test
    public void ensureQueueGettingFlushedAndNotAppliedUponRollback() {
        try (Transaction tx = db.beginTx()) {
            AsyncQueueHolder asyncQueueHolder = db.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
            asyncQueueHolder.add((graphDatabaseService, log) -> graphDatabaseService.createNode());

            tx.failure();
        }
        finishQueueAndWait();

        try (Transaction ignored = db.beginTx()) {
            assertEquals(0, Iterables.count(db.getAllNodes()));
        }
    }

    @Test
    public void ensureTxWithoutAsyncActionWorks() {
        try (Transaction tx = db.beginTx()) {
            db.createNode();
            tx.success();
        }
        finishQueueAndWait();

        try (Transaction ignored = db.beginTx()) {
            assertEquals(1, Iterables.count(db.getAllNodes()));
        }
    }

    private boolean runWithExecutorService(int times, Consumer consumer) {

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(8);
            for (int i = 0; i < times; i++) {
                int finalI = i;
                executorService.submit(() -> {
                            try {
                                consumer.accept(finalI);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                );
            }
            executorService.shutdown();
            boolean regularTermination = executorService.awaitTermination(30, SECONDS);
            finishQueueAndWait();
            return regularTermination;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void finishQueueAndWait() {
        AsyncQueueHolder asyncQueueHolder = db.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.stop();
        assertTrue(asyncQueueHolder.isQueueEmpty());
        assertTrue(asyncQueueHolder.isInBoundEmpty());
    }

}
