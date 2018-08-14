package org.neo4j.asyncaction;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * @author Stefan Armbruster
 */
public class ProceduresTests {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Procedures.class);
//    Neo4jResource neo4j = new Neo4jResource(userLogProvider: new AssertableLogProvider(false), internalLogProvider: new AssertableLogProvider(false))

    @Test
    public void shouldAsyncCreationOfRelationshipsWork() {

        neo4j.getGraphDatabaseService().execute("CREATE (a), (b) WITH a,b UNWIND range(1,20) as x CALL async.createRelationship(a, b, 'KNOWS') RETURN a,b");
        finishQueueAndWait();

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH ()-[r:KNOWS]->() RETURN count(r) as count").columnAs("count"));
        assertEquals(20, count);
    }

    @Test
    public void shouldAsyncCreationOfRelationshipViaCypherWork() {

        neo4j.getGraphDatabaseService().execute("CREATE (a), (b) \n" +
                "WITH a,b\n" +
                "UNWIND range(1,20) as x\n" +
                "CALL async.cypher(\"with $start as start, $end as end create (start)-[:KNOWS]->(end)\", {start:a, end:b})  \n" +
                "RETURN a,b");
        finishQueueAndWait();

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("MATCH ()-[r:KNOWS]->() RETURN count(r) as count").columnAs("count"));
        assertEquals(20, count);
    }

    @Test
    public void shouldConnectingToDenseNodeWorkWithCypher() {
        neo4j.getGraphDatabaseService().execute("create index on :Person(id)");
        neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'})");

        boolean regularTermination = runWithExecutorService(1000, o -> {
            neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'}) \n" +
                    "MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})\n" +
                    "MERGE (rnd)-[:KNOWS]->(dense)");
        });

        assertTrue(regularTermination);

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("match (p:Person) return count(p) as c").columnAs("c"));
        assertTrue( count < 1000);

        count = Iterators.single(neo4j.getGraphDatabaseService().execute("match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c").columnAs("c"));
        assertTrue( count < 1000);
    }

    @Test
    public void testConnectingToDenseNodeUsingAsync() {
        neo4j.getGraphDatabaseService().execute("create index on :Person(id)");
        neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'})");
        boolean regularTermination = runWithExecutorService(1000, index -> {
            neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'}) \n" +
                    "    CREATE (rnd:Person{id:$id})\n" +
                    "    WITH dense, rnd \n" +
                    "    CALL async.createRelationship(rnd, dense, 'KNOWS') \n" +
                    "    RETURN dense, rnd", Collections.singletonMap("id", "person_" + index));
        });
        assertTrue(regularTermination);

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("match (p:Person) return count(p) as c").columnAs("c"));
        assertTrue( count == 1001);

        count = Iterators.single(neo4j.getGraphDatabaseService().execute("match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c").columnAs("c"));
        assertTrue( count == 1000);
    }

    @Test
    public void testConnectingToDensoNodeUsingAsyncMerge() {
        neo4j.getGraphDatabaseService().execute("create index on :Person(id)");
        neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'})");
        boolean regularTermination = runWithExecutorService(1000, index -> {
            neo4j.getGraphDatabaseService().execute("MERGE (dense:Person{id:'dense'}) \n" +
                    "    MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})\n" +
                    "    WITH dense, rnd \n" +
                    "    CALL async.mergeRelationship(rnd, dense, 'KNOWS') \n" +
                    "    RETURN dense, rnd");
        });
        assertTrue(regularTermination);

        long count = Iterators.single(neo4j.getGraphDatabaseService().execute("match (p:Person) return count(p) as c").columnAs("c"));
        assertTrue( count < 800 );

        count = Iterators.single(neo4j.getGraphDatabaseService().execute("match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c").columnAs("c"));
        assertTrue( count < 800);

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
            boolean regularTermination = executorService.awaitTermination(10, SECONDS);
            finishQueueAndWait();
            return regularTermination;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void finishQueueAndWait() {
        AsyncQueueHolder asyncQueueHolder = ((GraphDatabaseAPI) neo4j.getGraphDatabaseService()).getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.stop();

        while (!asyncQueueHolder.isQueueEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
