package org.neo4j.asyncaction

import org.junit.Rule
import org.neo4j.extension.spock.Neo4jResource
import org.neo4j.graphdb.Result
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.AssertableLogProvider
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static java.util.concurrent.TimeUnit.SECONDS

/**
 * @author Stefan Armbruster
 */
class ProceduresSpec extends Specification {

    @Rule
    @Delegate(interfaces = false)
    Neo4jResource neo4j = new Neo4jResource(userLogProvider: new AssertableLogProvider(), internalLogProvider: new AssertableLogProvider())

    def "should async creation of relationships work"() {
        when:
        Result result = """CREATE (a), (b) 
WITH a,b
UNWIND range(1,20) as x
CALL async.createRelationship(a, b, 'KNOWS')  
RETURN a,b""".cypher()

        def asyncQueueHolder = ((GraphDatabaseAPI)graphDatabaseService).dependencyResolver.resolveDependency(AsyncQueueHolder)
        asyncQueueHolder.stop() // send poison
        sleep 10

        userLogProvider.print(System.out)


        then:
        "MATCH ()-[r:KNOWS]->() RETURN count(r) as count".cypher()[0].count == 20
    }

    def "test connecting to dense node with plain cypher"() {
        setup:
        "create index on :Person(id)".cypher()
        "MERGE (dense:Person{id:'dense'})".cypher()
        ExecutorService executorService = Executors.newFixedThreadPool(8)

        when:

        (1..1000).each {
            executorService.submit {
                """MERGE (dense:Person{id:'dense'}) 
MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})
MERGE (rnd)-[:KNOWS]->(dense)""".cypher()
            }
        }
        executorService.shutdown()

        then:
        executorService.awaitTermination(10, SECONDS)
        "match (p:Person) return count(p) as c".cypher()[0].c < 1000
        "match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c".cypher()[0].c < 1000

    }


    def "test connecting to dense node using async"() {
        setup:
        "create index on :Person(id)".cypher()
        "MERGE (dense:Person{id:'dense'})".cypher()
        ExecutorService executorService = Executors.newFixedThreadPool(8)

        when:
        (1..1000).each {
            executorService.submit {
                try {
                    graphDatabaseService.execute("""MERGE (dense:Person{id:'dense'}) 
    MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})
    WITH dense, rnd 
    CALL async.createRelationship(rnd, dense, 'KNOWS') 
    RETURN dense, rnd""")
                } catch (Exception e) {
                    println e
                    throw new RuntimeException(e)
                }
            }
        }
        executorService.shutdown()
        executorService.awaitTermination(10, SECONDS)

        def asyncQueueHolder = ((GraphDatabaseAPI)graphDatabaseService).dependencyResolver.resolveDependency(AsyncQueueHolder)
        asyncQueueHolder.stop() // send poison
        sleep 10
        userLogProvider.print(System.out)

        then:
        "match (p:Person) return count(p) as c".cypher()[0].c < 1000
        "match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c".cypher()[0].c > 900

    }

    def "test connecting to dense node using async merge"() {
        setup:
        "create index on :Person(id)".cypher()
        "MERGE (dense:Person{id:'dense'})".cypher()
        ExecutorService executorService = Executors.newFixedThreadPool(8)

        when:
        (1..1000).each {
            executorService.submit {
                try {
                    graphDatabaseService.execute("""MERGE (dense:Person{id:'dense'}) 
    MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})
    WITH dense, rnd 
    CALL async.mergeRelationship(rnd, dense, 'KNOWS') 
    RETURN dense, rnd""")
                } catch (Exception e) {
                    println e
                    throw new RuntimeException(e)
                }
            }
        }
        executorService.shutdown()
        executorService.awaitTermination(10, SECONDS)

        def asyncQueueHolder = ((GraphDatabaseAPI)graphDatabaseService).dependencyResolver.resolveDependency(AsyncQueueHolder)
        asyncQueueHolder.stop() // send poison
        sleep 10
        userLogProvider.print(System.out)

        then:
        "match (p:Person) return count(p) as c".cypher()[0].c < 1000
        "match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c".cypher()[0].c < 1000
    }
}
