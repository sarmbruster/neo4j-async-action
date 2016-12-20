package org.neo4j.asyncaction

import org.junit.Rule
import org.neo4j.extension.spock.Neo4jResource
import org.neo4j.graphdb.Result
import org.neo4j.kernel.internal.GraphDatabaseAPI
import spock.lang.Specification

/**
 * @author Stefan Armbruster
 */
class ProceduresSpec extends Specification {

    @Rule
    @Delegate(interfaces = false)
    Neo4jResource neo4j = new Neo4jResource()

    def "should async creation of relationships work"() {
        when:
        Result result = """CREATE (a), (b) 
WITH a,b
UNWIND range(1,20) as x
CALL async.createRelationship(a, b, 'KNOWS')  
RETURN a,b""".cypher()

        /*def asyncQueueHolder = ((GraphDatabaseAPI)graphDatabaseService).dependencyResolver.resolveDependency(AsyncQueueHolder)
        asyncQueueHolder.stop() // send poison */
        sleep 10000


        then:
        "MATCH ()-[r:KNOWS]->() RETURN count(r) as count".cypher()[0].count == 1

    }
}
