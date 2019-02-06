package org.neo4j.asyncaction;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

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

    @Rule
    public Neo4jRule neo4j = new Neo4jRule().withProcedure(Procedures.class);

    @Test
    public void verifyNoDeadlock() throws InterruptedException {

        final long languageNodeId = Iterators.single(neo4j.getGraphDatabaseService().execute("CREATE (a:LANGUAGE) RETURN id(a) as id").columnAs("id"));

        List<Long> patentIds = new ArrayList<>();

        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            for (int i = 0; i < 100; i++) {
                final Long nodeId = neo4j.getGraphDatabaseService().createNodeId();
                patentIds.add(nodeId);
            }
            tx.success();
        }

        try (final Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            patentIds.parallelStream().forEach((patentId) -> {
                final Session session = driver.session();
                final StatementResult result = session.run("CALL async.createRelationshipByIds($languageId, $patentId, 'HAS')",
                        MapUtil.map(
                            "languageId", languageNodeId,
                            "patentId", patentId
                        )
                );
                final ResultSummary consume = result.consume();
            });
        }

        finishQueueAndWait();

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH ()-[r:HAS]->() RETURN count(r) AS count").columnAs("count"));
        Assert.assertEquals(100, count);
    }

    @Test
    public void verifyPropertiesAreAddedTest() {

        neo4j.getGraphDatabaseService().execute("CREATE (a), (b) WITH a,b UNWIND range(1,20) AS x CALL async.createRelationship(a, b, 'KNOWS', {test : 2}) RETURN a,b");
        finishQueueAndWait();

        for (Map<String, Object> result : Iterators.asIterable(neo4j.getGraphDatabaseService().execute("MATCH ()-[r:KNOWS]->() RETURN r"))) {

            final Transaction transaction = neo4j.getGraphDatabaseService().beginTx();

            Assert.assertEquals(1, result.keySet().size());
            Assert.assertEquals(1, ((Relationship) result.get("r")).getAllProperties().keySet().size());
            Assert.assertEquals(2L, ((Relationship) result.get("r")).getProperty("test"));
            transaction.success();
            transaction.close();

        }
    }

    @Test
    public void shouldAsyncCreationOfRelationshipsWork() {

        neo4j.getGraphDatabaseService().execute("CREATE (a), (b) WITH a,b UNWIND range(1,20) AS x CALL async.createRelationship(a, b, 'KNOWS', {}) RETURN a,b");
        finishQueueAndWait();

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count").columnAs("count"));
        assertEquals(20, count);

    }

    @Test
    public void shouldAsyncCreationOfRelationshipViaCypherWork() {

        neo4j.getGraphDatabaseService().execute("CREATE (a), (b) \n" +
                "WITH a,b\n" +
                "UNWIND range(1,20) as x\n" +
                "CALL async.cypher(\"with $start as start, $end as end create (start)-[:KNOWS]->(end)\", {start:a, end:b})\n" +
                "RETURN a,b");
        finishQueueAndWait();

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count").columnAs("count"));
        assertEquals(20, count);
    }

    @Test
    public void shouldConnectingToDenseNodeWorkWithCypher() {

        neo4j.getGraphDatabaseService().execute("CREATE INDEX ON :Person(id)");
        neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'})");

        boolean regularTermination = runWithExecutorService(1000, o -> {
            neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'}) \n" +
                    "MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})\n" +
                    "MERGE (rnd)-[:KNOWS]->(dense)");
        });

        assertTrue(regularTermination);

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH (p:Person) RETURN count(p) AS c").columnAs("c"));
        assertTrue(count < 1000);

        count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH (p:Person{id:'dense'}) RETURN size((p)<-[:KNOWS]-()) AS c").columnAs("c"));
        assertTrue(count < 1000);
    }

    @Test
    public void testConnectingToDenseNodeUsingAsync() {

        neo4j.getGraphDatabaseService().execute("CREATE INDEX ON :Person(id)");
        neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'})");
        boolean regularTermination = runWithExecutorService(1000, index -> {
            neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'}) \n" +
                    "    CREATE (rnd:Person{id:$id})\n" +
                    "    WITH dense, rnd \n" +
                    "    CALL async.createRelationship(rnd, dense, 'KNOWS', {}) \n" +
                    "    RETURN dense, rnd", Collections.singletonMap("id", "person_" + index));
        });
        assertTrue(regularTermination);

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH (p:Person) RETURN count(p) AS c").columnAs("c"));
        assertTrue(count == 1001);

        count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH (p:Person{id:'dense'}) RETURN size((p)<-[:KNOWS]-()) AS c").columnAs("c"));
        assertTrue(count == 1000);
    }

    @Test
    public void testConnectingToDenseNodeUsingAsyncMerge() {

        neo4j.getGraphDatabaseService().execute("CREATE INDEX ON :Person(id)");
        neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'})");
        boolean regularTermination = runWithExecutorService(1000, index -> {
            neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'}) \n" +
                    "    MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})\n" +
                    "    WITH dense, rnd \n" +
                    "    CALL async.mergeRelationship(rnd, dense, 'KNOWS') \n" +
                    "    RETURN dense, rnd");
        });

        assertTrue(regularTermination);

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH (p:Person) RETURN count(p) AS c").columnAs("c"));
        assertTrue(count < 800);

        count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH (p:Person{id:'dense'}) RETURN size((p)<-[:KNOWS]-()) AS c").columnAs("c"));
        assertTrue(count < 800);

    }

    @Test
    public void ensureQueueGettingFlushedAndNotAppliedUponRollback() {
        GraphDatabaseAPI db = (GraphDatabaseAPI) neo4j.getGraphDatabaseService();
        try (Transaction tx = db.beginTx()) {
            AsyncQueueHolder asyncQueueHolder = db.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
            asyncQueueHolder.add((graphDatabaseService, log) -> graphDatabaseService.createNode());

            tx.failure();
        }
        finishQueueAndWait();

        try (Transaction tx = db.beginTx()) {
            assertEquals(0, Iterables.count(db.getAllNodes()));
        }
    }

    @Test
    public void ensureTxWithoutAsyncActionWorks() {
        GraphDatabaseAPI db = (GraphDatabaseAPI) neo4j.getGraphDatabaseService();
        try (Transaction tx = db.beginTx()) {
            db.createNode();
            tx.success();
        }
        finishQueueAndWait();

        try (Transaction tx = db.beginTx()) {
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
        AsyncQueueHolder asyncQueueHolder = ((GraphDatabaseAPI) neo4j.getGraphDatabaseService()).getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.stop();
    }

}
