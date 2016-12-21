package org.neo4j.asyncaction.command;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

/**
 * used to indicate end of data in queues
 */
public class PoisonCommand extends GraphCommand {

    public static final PoisonCommand POISON = new PoisonCommand();

    private PoisonCommand() {
        super(null);
    }

    @Override
    public String toString() {
        return "[POISON]";
    }

    @Override
    public void run(GraphDatabaseService graphDatabaseService, Log log) {
        // do nothing
    }
}
