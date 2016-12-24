package org.neo4j.asyncaction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.procedure.Mode.READ;

/**
 * @author Stefan Armbruster
 */
public class Procedures {

    @Context
    public GraphDatabaseAPI api;

    @Procedure(name = "async.createRelationship", mode = READ)
    @Description("create relationships asynchronously to prevent locking issues")
    public void asyncCreateRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        addToAsyncQueue((graphDatabaseService, log) -> startNode.createRelationshipTo(endNode, RelationshipType.withName(relationshipType)));
    }

    @Procedure(name = "async.mergeRelationship", mode = READ)
    @Description("merge relationships asynchronously to prevent locking issues")
    public void asyncMergeRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType) {
        addToAsyncQueue((graphDatabaseService, log) -> {
            RelationshipType rt = RelationshipType.withName(relationshipType);
            int startDegree = startNode.getDegree(rt);
            int endDegree = endNode.getDegree(rt);
            boolean startNodeCheaper = Math.min(startDegree, endDegree) == startDegree;

            final Node cheaperNode = startNodeCheaper ? startNode : endNode;
            final Node expensiverNode = startNodeCheaper ? endNode : startNode;
            final Direction direction = startNodeCheaper ? OUTGOING : INCOMING;

            boolean relationshipExists = StreamSupport.stream(cheaperNode.getRelationships(rt, direction).spliterator(), false)
                    .anyMatch(relationship -> relationship.getOtherNode(cheaperNode).equals(expensiverNode));

            if (!relationshipExists) {
                startNode.createRelationshipTo(endNode, RelationshipType.withName(relationshipType));
            }
        });
    }

    @Procedure(name = "async.cypher", mode = READ)
    @Description("queue a cypher statement for async batched processing to prevent locking issues")
    public void asyncCypher(
            @Name("cypher") String cypherString,
            @Name("params") Map<String, Object> params) {
        addToAsyncQueue( (graphDatabaseService, log) -> {
            graphDatabaseService.execute(cypherString, params);
        });
    }

    private void addToAsyncQueue(GraphCommand command) {
        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add(command);
    }

}