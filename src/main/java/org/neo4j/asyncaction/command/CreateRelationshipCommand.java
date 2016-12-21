package org.neo4j.asyncaction.command;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

/**
 * @author Stefan Armbruster
 */
public class CreateRelationshipCommand extends GraphCommand {

    private final Node startNode;
    private final Node endNode;
    private final String relationshipType;

    public CreateRelationshipCommand(Node startNode, Node endNode, String relationshipType, KernelTransaction kernelTransaction) {
        super(kernelTransaction);
        this.startNode = startNode;
        this.endNode = endNode;
        this.relationshipType = relationshipType;
    }

    public Node getStartNode() {
        return startNode;
    }

    public Node getEndNode() {
        return endNode;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreateRelationshipCommand that = (CreateRelationshipCommand) o;

        if (startNode != null ? !startNode.equals(that.startNode) : that.startNode != null) return false;
        if (endNode != null ? !endNode.equals(that.endNode) : that.endNode != null) return false;
        return relationshipType != null ? relationshipType.equals(that.relationshipType) : that.relationshipType == null;
    }

    @Override
    public int hashCode() {
        int result = startNode != null ? startNode.hashCode() : 0;
        result = 31 * result + (endNode != null ? endNode.hashCode() : 0);
        result = 31 * result + (relationshipType != null ? relationshipType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("(%d)-[:%s]->(%d)", startNode.getId(), relationshipType, endNode.getId());
    }

    @Override
    public void run(GraphDatabaseService graphDatabaseService, Log log) {
        try {
            getStartNode().createRelationshipTo(getEndNode(), RelationshipType.withName(getRelationshipType()));
        } catch (NotFoundException e) {
            log.debug("oops" , e);
        }
    }
}
