package org.apache.spark.cypher

import org.apache.spark.cypher.SparkTable.DataFrameTable
import org.apache.spark.cypher.adapters.RelationalGraphAdapter
import org.apache.spark.graph.api._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): CypherSession = new SparkCypherSession(sparkSession)

  private[spark] def createInternal(implicit sparkSession: SparkSession): SparkCypherSession = new SparkCypherSession(sparkSession)
}

/**
  * Default [[CypherSession]] implementation.
  *
  * This class is the main entry point for working with the spark-cypher module.
  * It wraps a [[SparkSession]] and allows to run Cypher queries over graphs represented as [[org.apache.spark.sql.DataFrame]]s.
  */
private[spark] class SparkCypherSession(override val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with CypherSession {

  override type Result = RelationalCypherResult[DataFrameTable]
  override type Records = SparkCypherRecords

  implicit def sparkCypherSession: SparkCypherSession = this

  override val records: SparkCypherRecordsFactory = SparkCypherRecordsFactory()

  override val graphs: RelationalCypherGraphFactory[DataFrameTable] = {
    new RelationalCypherGraphFactory[DataFrameTable]() {
      override implicit val session: RelationalCypherSession[DataFrameTable] = sparkCypherSession
    }
  }

  override def entityTables: RelationalEntityTableFactory[DataFrameTable] = {
    throw UnsupportedOperationException("Graph construction with `CONSTRUCT` is not supported in Cypher 9")
  }

  override def createGraph(nodes: Seq[DataFrame], relationships: Seq[DataFrame] = Seq.empty): PropertyGraph = {
    require(nodes.groupBy(_.labels).forall(_._2.size == 1),
      "There can be at most one NodeFrame per label set")
    require(relationships.groupBy(_.relationshipType).forall(_._2.size == 1),
      "There can be at most one RelationshipFrame per relationship type")
    RelationalGraphAdapter(this, nodes, relationships)
  }

  def cypher(graph: PropertyGraph, query: String): CypherResult = cypher(graph, query, Map.empty)

  override def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult = {
    val relationalGraph = toRelationalGraph(graph)
    SparkCypherResult(relationalGraph.cypher(query, CypherMap(parameters.toSeq: _*)).records)
  }

  private val DEFAULT_FORMAT = "parquet"

  override def load(path: String): PropertyGraph = ???

  override private[spark] def save(graph: PropertyGraph, path: String, saveMode: SaveMode): Unit = ???

  private def toRelationalGraph(graph: PropertyGraph): RelationalCypherGraph[DataFrameTable] = {
    graph match {
      case adapter: RelationalGraphAdapter => adapter.graph
      case other => throw IllegalArgumentException(
        expected = "A graph that has been created by `SparkCypherSession.createGraph`",
        actual = other.getClass.getSimpleName
      )
    }
  }

}
