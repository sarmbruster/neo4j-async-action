package org.neo4j.asyncaction;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
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

    @Context
    public Log log;

    @Procedure(name = "async.createRelationshipByIds", mode = READ)
    @Description("create relationships asynchronously based on node ids to prevent locking issues")
    public void createRelationshipOverId(
            @Name("startNode") Long startNodeId,
            @Name("endNode") Long endNodeId,
            @Name("relationshipType") String relationshipType,
            @Name(value = "relationshipProperties", defaultValue = "") Map<String, Object> relationshipProperties) {
        asyncCreateRelationship(api.getNodeById(startNodeId), api.getNodeById(endNodeId), relationshipType, relationshipProperties);

    }

    @Procedure(name = "async.createRelationship", mode = READ)
    @Description("create relationships asynchronously to prevent locking issues")
    public void asyncCreateRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType,
            @Name(value = "relationshipProperties", defaultValue = "") Map<String, Object> relationshipProperties) {
        addToAsyncQueue((graphDatabaseService, log) -> {
            final Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(relationshipType));
            setAllProperties(relationship, relationshipProperties);
        });
    }

    private void setAllProperties(PropertyContainer propertyContainer, Map<String, Object> properties) {
        if (properties != null) {
            properties.entrySet().stream().forEach( entry -> propertyContainer.setProperty(entry.getKey(), entry.getValue()));
        }
    }

    @Procedure(name = "async.mergeRelationship", mode = READ)
    @Description("merge relationships asynchronously to prevent locking issues")
    public void asyncMergeRelationship(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipType") String relationshipType,
            @Name(value = "identifyingRelationshipProperties", defaultValue = "") Map<String, Object> identifyingRelationshipProperties,
            @Name(value = "onCreateProperties", defaultValue = "") Map<String, Object> onCreateProperties,
            @Name(value = "onMatchProperties", defaultValue = "") Map<String, Object> onMatchProperties
    ) {

        addToAsyncQueue((graphDatabaseService, log) -> {
            RelationshipType rt = RelationshipType.withName(relationshipType);
            int startDegree = startNode.getDegree(rt);
            int endDegree = endNode.getDegree(rt);
            boolean startNodeCheaper = Math.min(startDegree, endDegree) == startDegree;

            final Node cheaperNode = startNodeCheaper ? startNode : endNode;
            final Node expensiverNode = startNodeCheaper ? endNode : startNode;
            final Direction direction = startNodeCheaper ? OUTGOING : INCOMING;

            Optional<Relationship> optionalRelationship = StreamSupport.stream(cheaperNode.getRelationships(rt, direction).spliterator(), false)
                    .filter(relationship ->
                            relationship.getOtherNode(cheaperNode).equals(expensiverNode)
                                    && relationshipMatchesAllProperties(relationship, identifyingRelationshipProperties)).findAny();
            if (optionalRelationship.isPresent()) {
                setAllProperties(optionalRelationship.get(), onMatchProperties);
            } else {
                final Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(relationshipType));
                setAllProperties(relationship, identifyingRelationshipProperties);
                setAllProperties(relationship, onCreateProperties);
            }
        });
    }

    private boolean relationshipMatchesAllProperties(Relationship relationship, Map<String, Object> propertiesToMatch) {
        return propertiesToMatch.entrySet().stream().allMatch(propertyKeyValue -> propertyKeyValue.getValue().equals(relationship.getProperty(propertyKeyValue.getKey(), null)));
    }

    @Procedure(name = "async.cypher", mode = READ)
    @Description("queue a cypher statement for async batched processing to prevent locking issues")
    public void asyncCypher(
            @Name("cypher") String cypherString,
            @Name("params") Map<String, Object> params) {

        addToAsyncQueue((graphDatabaseService, log) -> {
            graphDatabaseService.execute(cypherString, params);
        });
    }

    private void addToAsyncQueue(GraphCommand command) {

        AsyncQueueHolder asyncQueueHolder = api.getDependencyResolver().resolveDependency(AsyncQueueHolder.class);
        asyncQueueHolder.add(command);
    }

}
