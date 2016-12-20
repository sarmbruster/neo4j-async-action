package org.neo4j.asyncaction;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;


/**
 * @author Stefan Armbruster
 */
public class Procedures {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction kernelTransaction;

    @Procedure(name = "async.createRelationship", mode = Mode.WRITE)
    public void asyncCreateRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add(startNode, endNode, relationshipType, kernelTransaction);
    }

}
