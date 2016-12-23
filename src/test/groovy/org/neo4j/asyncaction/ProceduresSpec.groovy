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
    Neo4jResource neo4j = new Neo4jResource(userLogProvider: new AssertableLogProvider(false), internalLogProvider: new AssertableLogProvider(false))

    def "should async creation of relationships work"() {
        when:
        Result result = """CREATE (a), (b) 
WITH a,b
UNWIND range(1,20) as x
CALL async.createRelationship(a, b, 'KNOWS')  
RETURN a,b""".cypher()

        finishQueueAndWait()
//        userLogProvider.print(System.out)

        then:
        "MATCH ()-[r:KNOWS]->() RETURN count(r) as count".cypher()[0].count == 20
    }

    def "should async creation of relationships via cypher work"() {
        when:
        Result result = '''CREATE (a), (b) 
WITH a,b
UNWIND range(1,20) as x
CALL async.cypher("with $start as start, $end as end create (start)-[:KNOWS]->(end)", {start:a, end:b})  
RETURN a,b'''.cypher()

        finishQueueAndWait()
//        userLogProvider.print(System.out)

        then:
        "MATCH ()-[r:KNOWS]->() RETURN count(r) as count".cypher()[0].count == 20
    }

    def "test connecting to dense node with plain cypher"() {
        setup:
        "create index on :Person(id)".cypher()
        "MERGE (dense:Person{id:'dense'})".cypher()

        when:
        boolean regularTermination = runWithExecutorService(1000) { num ->
            """MERGE (dense:Person{id:'dense'}) 
MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})
MERGE (rnd)-[:KNOWS]->(dense)""".cypher()
        }

        then:
        regularTermination
        "match (p:Person) return count(p) as c".cypher()[0].c < 1000
        "match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c".cypher()[0].c < 1000
    }


    def "test connecting to dense node using async"() {
        setup:
        "create index on :Person(id)".cypher()
        "MERGE (dense:Person{id:'dense'})".cypher()

        when:
        boolean regularTermination = runWithExecutorService(1000) { num ->
            graphDatabaseService.execute('''MERGE (dense:Person{id:'dense'}) 
    CREATE (rnd:Person{id:$id})
    WITH dense, rnd 
    CALL async.createRelationship(rnd, dense, 'KNOWS') 
    RETURN dense, rnd''', [id: "person_${num}".toString()])
        }
//        userLogProvider.print(System.out)

        then:
        regularTermination
        "match (p:Person) return count(p) as c".cypher()[0].c == 1001
        "match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c".cypher()[0].c == 1000

    }

    def "test connecting to dense node using async merge"() {
        setup:
        "create index on :Person(id)".cypher()
        "MERGE (dense:Person{id:'dense'})".cypher()

        when:
        boolean regularTermination = runWithExecutorService(1000) {
            graphDatabaseService.execute('''MERGE (dense:Person{id:'dense'}) 
    MERGE (rnd:Person{id:'person_' +toInt(rand()*1000)})
    WITH dense, rnd 
    CALL async.mergeRelationship(rnd, dense, 'KNOWS') 
    RETURN dense, rnd''')
        }
//        userLogProvider.print(System.out)

        then:
        regularTermination
        "match (p:Person) return count(p) as c".cypher()[0].c < 800
        "match (p:Person{id:'dense'}) return size((p)<-[:KNOWS]-()) as c".cypher()[0].c < 800
        "match (p:Person) return size((p)-[:KNOWS]->()) as c".cypher()[0].c == 0
    }

    private boolean runWithExecutorService(int times, Closure closure) {
        ExecutorService executorService = Executors.newFixedThreadPool(8)
        (1..times).each { num ->
            executorService.submit {
                try {
                    closure.call(num)
                } catch (Exception e) {
                    println e
                    throw new RuntimeException(e)
                }
            }
        }
        executorService.shutdown()
        def regularTermination = executorService.awaitTermination(10, SECONDS)
        finishQueueAndWait()
        return regularTermination
    }

    private void finishQueueAndWait() {
        def asyncQueueHolder = ((GraphDatabaseAPI) graphDatabaseService).dependencyResolver.resolveDependency(AsyncQueueHolder)
        asyncQueueHolder.stop() // send poison
        while (!asyncQueueHolder.outboundQueue.empty) {
            sleep 10
        }
    }
}