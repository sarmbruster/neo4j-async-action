package org.neo4j.asyncaction;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.Collections;
import java.util.Map;

@State(Scope.Benchmark)
public class GraphDatabaseState {


    private GraphDatabaseService graphDatabaseService;

    public GraphDatabaseService getGraphDatabaseService() {
        return graphDatabaseService;
    }

    @Setup(Level.Invocation)
    public final void setup() {
        graphDatabaseService = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(getGraphDatabaseConfig())
                .newGraphDatabase();
        setupGraphDatabase(graphDatabaseService);
    }

    @TearDown(Level.Invocation)
    public final void tearDown() {

        AsyncQueueHolder asyncQueueHolder = ((GraphDatabaseAPI) graphDatabaseService).getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.finishQueueAndWait();

        try (Transaction tx = graphDatabaseService.beginTx()) {
            long numberOfNodes = Iterables.count(graphDatabaseService.getAllNodes());
            long numberOfRels = Iterables.count(graphDatabaseService.getAllRelationships());
            System.out.println(" we have " + numberOfNodes + " nodes and " + numberOfRels + " relationships");
            tx.success();
        }
        graphDatabaseService.shutdown();
//            System.out.println("db stopped.");
    }

    public Map<String, String> getGraphDatabaseConfig() {
        return Collections.EMPTY_MAP;
    }

    void setupGraphDatabase(GraphDatabaseService graphDatabaseService) {
        try {
            org.neo4j.kernel.impl.proc.Procedures procedures = ((GraphDatabaseAPI) graphDatabaseService).getDependencyResolver().resolveDependency(org.neo4j.kernel.impl.proc.Procedures.class);
            procedures.registerProcedure(Procedures.class);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
//        Procedures procedures = ((GraphDatabaseAPI) graphDatabaseService).getDependencyResolver().resolveDependency(Procedures.class);
//        procedures.
    }
}

