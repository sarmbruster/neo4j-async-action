package org.neo4j.asyncaction;

import org.neo4j.asyncaction.command.CreateRelationshipCommand;
import org.neo4j.asyncaction.command.MergeNodeAndRelationshipCommand;
import org.neo4j.asyncaction.command.MergeRelationshipCommand;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

/**
 * @author Stefan Armbruster
 */
public class Procedures {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction kernelTransaction;

    @Procedure(name = "async.createRelationship", mode = Mode.WRITE)
    @Description("create relationships asynchronously to prevent locking issues")
    public void asyncCreateRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add(new CreateRelationshipCommand(startNode, endNode, relationshipType, kernelTransaction));
    }

    @Procedure(name = "async.mergeRelationship", mode = Mode.WRITE)
    @Description("merge relationships asynchronously to prevent locking issues")
    public void asyncMergeRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add(new MergeRelationshipCommand(startNode, endNode, relationshipType, kernelTransaction));
    }

    @Procedure(name ="async.mergeNodeAndRelationship", mode = Mode.WRITE)
    @Description("merge node an relationship in separate thread")
    public void asyncMergeNodeAndRelationship(
            @Name("startNode") Node startNode,
            @Name("relationshipType") String relationshipType,
            @Name("label") String label,
            @Name("nodeKey") String key,
            @Name("nodeValue") String value ) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add(new MergeNodeAndRelationshipCommand(startNode, relationshipType, label, key, value, kernelTransaction));
    }

}