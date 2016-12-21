package org.neo4j.asyncaction.command;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

/**
 * @author Stefan Armbruster
 */
public abstract class GraphCommand {
    public static CreateRelationshipCommand POISON = new CreateRelationshipCommand(null, null, null, null);

    protected final KernelTransaction kernelTransaction;

    public GraphCommand(KernelTransaction kernelTransaction) {
        this.kernelTransaction = kernelTransaction;
    }

    public KernelTransaction getKernelTransaction() {
        return kernelTransaction;
    }

    public abstract void run(GraphDatabaseService graphDatabaseService, Log log);
}
