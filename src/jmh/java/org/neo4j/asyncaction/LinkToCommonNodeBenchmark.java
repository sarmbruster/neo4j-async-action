package org.neo4j.asyncaction;

import org.neo4j.graphdb.GraphDatabaseService;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LinkToCommonNodeBenchmark {

    @Benchmark
    public void viaRegularCypher(GraphDatabaseState state) {
        runActionViaExecutorService(state.getGraphDatabaseService(), 100,
                "MATCH (d:Dense) " +
                        "UNWIND range(0,1000) AS x " +
                "CREATE (:Person{id:$id * 1000 + x})-[:CONNECTED_TO]->(d)");
    }

    @Benchmark
    public void viaAsyncCypher(GraphDatabaseState state) {
        try {

            runActionViaExecutorService(state.getGraphDatabaseService(), 100,
                    "MATCH (d:Dense) " +
                            "UNWIND range(0,1000) AS x " +
                            "CREATE (p:Person{id:$id*1000+x}) " +
                            "CALL async.createRelationship(p, d, 'CONNECTED_TO')");
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    private void runActionViaExecutorService(GraphDatabaseService db, int numberInvocations, String query) {
        try {
            db.execute("CREATE (:Dense)");
            ExecutorService executorService = Executors.newWorkStealingPool();
            for (int i = 0; i < numberInvocations; i++ ) {
                int finalI = i;
                executorService.submit(() -> db.execute(query, Collections.singletonMap("id", finalI)));
            }

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
