package org.neo4j.asyncaction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.stream.StreamSupport;

/**
 * @author Stefan Armbruster
 */
public class Procedures {

    @Context
    public GraphDatabaseAPI api;

    @Procedure(name = "async.createRelationship", mode = Mode.WRITE)
    @Description("create relationships asynchronously to prevent locking issues")
    public void asyncCreateRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add((graphDatabaseService, log) -> startNode.createRelationshipTo(endNode, RelationshipType.withName(relationshipType)));
    }

    @Procedure(name = "async.mergeRelationship", mode = Mode.WRITE)
    @Description("merge relationships asynchronously to prevent locking issues")
    public void asyncMergeRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add((graphDatabaseService, log) -> {
            RelationshipType rt = RelationshipType.withName(relationshipType);
            int startDegree = startNode.getDegree(rt);
            int endDegree = endNode.getDegree(rt);
            boolean startNodeCheaper = Math.min(startDegree, endDegree) == startDegree;

            boolean relationshipExists = false;
            if (startNodeCheaper) {
                relationshipExists = StreamSupport.stream(startNode.getRelationships(rt, Direction.OUTGOING).spliterator(), false)
                        .anyMatch(relationship -> relationship.getEndNode().equals(endNode));
            } else {
                relationshipExists = StreamSupport.stream(endNode.getRelationships(rt, Direction.INCOMING).spliterator(), false)
                        .anyMatch(relationship -> relationship.getStartNode().equals(startNode));
            }

            if (!relationshipExists) {
                startNode.createRelationshipTo(endNode, RelationshipType.withName(relationshipType));
            }

        });
    }
}
