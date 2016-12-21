package org.neo4j.asyncaction.command;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

/**
 * @author Stefan Armbruster
 */
public class MergeNodeAndRelationshipCommand extends GraphCommand {

    private final Node endNode;
    private final String relationshipType;
    private final String labelString;
    private final String key;
    private final String value;

    public MergeNodeAndRelationshipCommand(Node endNode, String relationshipType, String label, String key, String value, KernelTransaction kernelTransaction) {
        super(kernelTransaction);
        this.endNode = endNode;
        this.relationshipType = relationshipType;
        this.labelString = label;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run(GraphDatabaseService graphDatabaseService, Log log) {
        Label label = Label.label(this.labelString);
        Node startNode = graphDatabaseService.findNode(label, key, value);
        if (startNode==null) {
            startNode = graphDatabaseService.createNode(label);
            startNode.setProperty(key, value);
        }

        RelationshipType relationshipType = RelationshipType.withName(this.relationshipType);
        if (!relationshipExists(startNode, endNode, relationshipType)) {
            startNode.createRelationshipTo(endNode, relationshipType);
        }
    }

    private boolean relationshipExists(Node startNode, Node endNode, RelationshipType relationshipType) {
        for (Relationship r: startNode.getRelationships(relationshipType, Direction.OUTGOING)) {
            if (r.getEndNode().equals(endNode)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MergeNodeAndRelationshipCommand that = (MergeNodeAndRelationshipCommand) o;

        if (endNode != null ? !endNode.equals(that.endNode) : that.endNode != null) return false;
        if (relationshipType != null ? !relationshipType.equals(that.relationshipType) : that.relationshipType != null)
            return false;
        if (labelString != null ? !labelString.equals(that.labelString) : that.labelString != null) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = endNode != null ? endNode.hashCode() : 0;
        result = 31 * result + (relationshipType != null ? relationshipType.hashCode() : 0);
        result = 31 * result + (labelString != null ? labelString.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
