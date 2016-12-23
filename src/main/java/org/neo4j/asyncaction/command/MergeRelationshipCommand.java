package org.neo4j.asyncaction.command;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.stream.StreamSupport;

/**
 * @author Stefan Armbruster
 */
public class MergeRelationshipCommand extends CreateRelationshipCommand {

    public MergeRelationshipCommand(Node startNode, Node endNode, String relationshipType, KernelTransaction kernelTransaction) {
        super(startNode, endNode, relationshipType, kernelTransaction);
    }

    @Override
    public void run(GraphDatabaseService graphDatabaseService, Log log) {
        RelationshipType rt = RelationshipType.withName(getRelationshipType());
        int startDegree = getStartNode().getDegree(rt);
        int endDegree = getEndNode().getDegree(rt);
        boolean startNodeCheaper = Math.min(startDegree, endDegree) == startDegree;

        boolean relationshipExists = false;
        if (startNodeCheaper) {
            relationshipExists = StreamSupport.stream(getStartNode().getRelationships(rt, Direction.OUTGOING).spliterator(), false)
                    .anyMatch(relationship -> relationship.getEndNode().equals(getEndNode()));
        } else {
            relationshipExists = StreamSupport.stream(getEndNode().getRelationships(rt, Direction.INCOMING).spliterator(), false)
                    .anyMatch(relationship -> relationship.getStartNode().equals(getStartNode()));
        }

        if (!relationshipExists) {
            super.run(graphDatabaseService, log);
        }
    }
}
