package org.neo4j.asyncaction;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import java.util.function.BiConsumer;

/**
 * Created by stefan on 23.12.16.
 */
@FunctionalInterface
public interface GraphCommand extends BiConsumer<GraphDatabaseService, Log> {
}
