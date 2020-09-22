package cn.pandadb.itest

import org.junit.{After, Assert, Before, Test}
import org.neo4j.driver.types.Node
import org.neo4j.driver.{AuthTokens, GraphDatabase, Session}
import org.json4s.native.Serialization
import org.json4s.NoTypeHints

import scala.collection.JavaConversions._
import scala.util.Random
import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.duration.SECONDS

/*
data model: simulated scientific knowledge graph dataset, graph 500
workload: write heavy; read heavy; fifty-fifty; readonly
*/

class Performance {

  val driver = GraphDatabase.driver("bolt://db:8687", AuthTokens.basic("neo4j", "neo4j"))
  var sampleNodes: Iterable[Node] = null
  val sampleSize = 100
  val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
  val session = driver.session()
  val intervalSub = true


  def sampling(sampleSize: Int): Iterable[Node] = {
    val session = driver.session()
    val result = session.run(s"MATCH (n) return n, rand() as rand ORDER BY rand ASC Limit ${sampleSize}").list()
    assert(result.size() == sampleSize)
    val sampleNodes = result.map(s => s.get(0).asNode())
    session.close()
    sampleNodes
  }

  class NodeClone(n: Node, session: Session) extends Runnable {
    override def run() {
      var labelPart = new StringBuilder("")
      n.labels().foreach(l => labelPart ++= s":${l}")
      labelPart ++= ":_meta_panda_bench"
      implicit val formats = Serialization.formats(NoTypeHints)
      val attrPart = Serialization.write(n.asMap().toMap).replaceAll("\"(\\w+)\":", "$1:")
      session.run(s"Create (n${labelPart}${attrPart})")
    }
  }

  class NodeModify(n: Node, session: Session) extends Runnable {
    override def run() {
      var labelPart = new StringBuilder("")
      n.labels().foreach(l => labelPart ++= s":${l}")
      implicit val formats = Serialization.formats(NoTypeHints)
      val attrPart = Serialization.write(n.asMap().toMap).replaceAll("\"(\\w+)\":", "$1:")
      session.run(s"Match (n${labelPart}${attrPart}) Set n:_meta_panda_bench, n._meta_panda_bench = 'benchmarking'")
    }
  }

  class NodeDoubleFilter(n: Node, session: Session) extends Runnable {
    override def run() {
      var labelPart = new StringBuilder("")
      n.labels().take(1).foreach(l => labelPart ++= s":${l}")
      val attrPartRetain2 = n.asMap().toMap.filterKeys(!_.toLowerCase.contains("id")).take(2).map(kv => ("n."+kv._1,kv._2))
      if(attrPartRetain2.size() < 2) return
      implicit val formats = Serialization.formats(NoTypeHints)
      val wherePart = Serialization.write(attrPartRetain2)
        .replaceAll("\"(n\\.\\w+)\":", "$1:")
        .replace(':', '=')
        .replace(",", " AND ")
        .replace("{", "").replace("}", "")
      val q = s"Match (n${labelPart}) where ${wherePart} return count(n)"
      println(q)
      session.run(q)
    }
  }

  class NodeTrippleFilter(n: Node, session: Session) extends Runnable {
    override def run() {
      var labelPart = new StringBuilder("")
      n.labels().take(1).foreach(l => labelPart ++= s":${l}")
      val attrPartRetain3 = n.asMap().toMap.filterKeys(!_.toLowerCase.contains("id")).take(3).map(kv => ("n."+kv._1,kv._2))
      if(attrPartRetain3.size() < 3) return
      implicit val formats = Serialization.formats(NoTypeHints)
      val wherePart = Serialization.write(attrPartRetain3)
        .replaceAll("\"(n\\.\\w+)\":", "$1:")
        .replace(':', '=')
        .replace(",", " AND ")
        .replace("{", "").replace("}", "")
      val q = s"Match (n${labelPart}) where ${wherePart} return count(n)"
      println(q)
      session.run(q)
    }
  }

  class NodeRegexpStartWithFilter(n: Node, session: Session) extends Runnable {
    override def run() {
      var labelPart = new StringBuilder("")
      n.labels().take(1).foreach(l => labelPart ++= s":${l}")
      val attrPartRetain = n.asMap().toMap.filter(a => {
        ! a._1.toLowerCase.contains("id") && a._2.isInstanceOf[String] && a._2.asInstanceOf[String].length>5
      }).take(1).map(kv => ("n."+kv._1,kv._2)).head
      if (attrPartRetain == null) return
      val regexp = attrPartRetain._2.asInstanceOf[String].split(' ').head
      if (regexp.length <3) return
      val wherePart = s"${attrPartRetain._1} STARTS WITH '${regexp}'"
      val q = s"Match (n${labelPart}) where ${wherePart} return count(n)"
      println(q)
      session.run(q)
    }
  }

  class NodeRegexpEndWithFilter(n: Node, session: Session) extends Runnable {
    override def run() {
      var labelPart = new StringBuilder("")
      n.labels().take(1).foreach(l => labelPart ++= s":${l}")
      val attrPartRetain = n.asMap().toMap.filter(a => {
        ! a._1.toLowerCase.contains("id") && a._2.isInstanceOf[String] && a._2.asInstanceOf[String].length>5
      }).take(1).map(kv => ("n."+kv._1,kv._2)).head
      if (attrPartRetain == null) return
      val regexp = attrPartRetain._2.asInstanceOf[String].split(' ').last
      if (regexp.length <3) return
      val wherePart = s"${attrPartRetain._1} ENDS WITH '${regexp}'"
      val q = s"Match (n${labelPart}) where ${wherePart} return count(n)"
      println(q)
      session.run(q)
    }
  }

  @Before
  def init(): Unit = {
//    session.run("MATCH (n:_meta_panda_bench) DETACH DELETE n")
    sampleNodes = sampling(sampleSize)
  }

  @Test
  def creationBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeClone(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
//    assert(session.run("match (n:_meta_panda_bench) return COUNT(n)").next().get(0).asInt() == sampleSize)
    println(s"creationBench: elapsed time ${duration} ms")
  }

  @Test
  def modifyBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeModify(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
//    assert(session.run("match (n:_meta_panda_bench) return COUNT(n)").next().get(0).asInt() == sampleSize)
    println(s"modifyBench: elapsed time ${duration} ms")
  }

  @Test
  def doubleFilterBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeDoubleFilter(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"doubleFilterBench: elapsed time ${duration} ms")
  }

  @Test
  def tripleFilterBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeTrippleFilter(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"tripleFilterBench: elapsed time ${duration} ms")
  }

  @Test
  def regexpStartWithFilterBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeRegexpStartWithFilter(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"tripleFilterBench: elapsed time ${duration} ms")
  }

  @Test
  def regexpEndWithFilterBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeRegexpEndWithFilter(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"tripleFilterBench: elapsed time ${duration} ms")
  }

  @Test
  def writeHeavyBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    var i = 0
    sampleNodes.foreach(s => {
      i += 1
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeModify(s, session))
      if (i % 5 == 0) threadPool.submit(new NodeDoubleFilter(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"writeHeavyBench: elapsed time ${duration} ms")
  }

  @Test
  def readHeavyBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    var i = 0
    sampleNodes.foreach(s => {
      i += 1
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeDoubleFilter(s, session))
      if (i % 5 == 0) threadPool.submit(new NodeModify(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"readHeavyBench: elapsed time ${duration} ms")
  }

  @Test
  def readWriteBench(): Unit = {
    val ran = new Random()
    val t0 = System.currentTimeMillis()
    sampleNodes.foreach(s => {
      if (intervalSub) Thread.sleep(ran.nextInt(200))
      threadPool.submit(new NodeDoubleFilter(s, session))
      threadPool.submit(new NodeModify(s, session))
    })
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, SECONDS)
    val duration = System.currentTimeMillis() - t0
    println(s"readWriteBench: elapsed time ${duration} ms")
  }

  @After
  def close: Unit = {
//    session.run("MATCH (n:_meta_panda_bench) DETACH DELETE n")
    session.close()
    driver.close()
  }
}