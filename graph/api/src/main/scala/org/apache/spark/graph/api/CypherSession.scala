package org.apache.spark.graph.api

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

/**
  * Allows for creating and loading [[PropertyGraph]] instances and running Cypher-queries on them.
  * Wraps a [[SparkSession]].
  */
trait CypherSession {

  def sparkSession: SparkSession

  /**
    * Executes a Cypher query on the given input graph.
    *
    * @param graph [[PropertyGraph]] on which the query is executed
    * @param query Cypher query to execute
    */
  def cypher(graph: PropertyGraph, query: String): CypherResult

  /**
    * Executes a Cypher query on the given input graph.
    *
    * @param graph      [[PropertyGraph]] on which the query is executed
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    */
  def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult

  /**
    * Creates a [[PropertyGraph]] from a sequence of [[DataFrame]]s and [[DataFrame]]s.
    * At least one [[DataFrame]] has to be provided.
    *
    * For each label set and relationship type there can be ar most on [[DataFrame]] and
    * [[DataFrame]], respectively.
    *
    * @param nodes         [[DataFrame]]s that define the nodes in the graph
    * @param relationships [[DataFrame]]s that define the relationships in the graph
    */
  def createGraph(nodes: Seq[DataFrame], relationships: Seq[DataFrame] = Seq.empty): PropertyGraph

  /**
    * Creates a [[PropertyGraph]] from nodes and relationships.
    *
    * The given DataFrames need to adhere to column naming conventions:
    *
    * {{{
    * Id column:        `$ID`            (nodes and relationships)
    * SourceId column:  `$SOURCE_ID`     (relationships)
    * TargetId column:  `$TARGET_ID`     (relationships)
    *
    * Label columns:    `:{LABEL_NAME}`  (nodes)
    * RelType columns:  `:{REL_TYPE}`    (relationships)
    *
    * Property columns: `{Property_Key}` (nodes and relationships)
    * }}}
    *
    * @param nodes         node [[DataFrame]]
    * @param relationships relationship [[DataFrame]]
    */
  def createGraph(nodes: DataFrame, relationships: DataFrame): PropertyGraph = {
    if (nodes.isNodeFrame && relationships.isRelationshipFrame) {
      createGraph(Seq(nodes), Seq(relationships))
    } else {
      val trueLit = functions.lit(true)
      val falseLit = functions.lit(false)

      val labelColumns = nodes.columns.filter(_.startsWith(":")).toSet
      val labelSets = labelColumns.subsets().toSet + Set.empty
      val nodeFrames = labelSets.map { labelSetColumns =>
        val predicate = labelColumns.map {
          case labelColumn if labelSetColumns.contains(labelColumn) => nodes.col(labelColumn) === trueLit
          case labelColumn => nodes.col(labelColumn) === falseLit
        }.reduce(_ && _)
        val labelSet = labelSetColumns.map(_.substring(1))
        nodes.filter(predicate).toNodeFrame(labelSet)
      }

      val relTypeColumns = relationships.columns.filter(_.startsWith(":")).toSet
      val relFrames = relTypeColumns.map { relTypeColumn =>
        val predicate = relationships.col(relTypeColumn) === trueLit
        val relationshipType = relTypeColumn.substring(1)
        relationships.filter(predicate).toRelationshipFrame(relationshipType)
      }

      createGraph(nodeFrames.toSeq, relFrames.toSeq)
    }
  }

  /**
    * Loads a [[PropertyGraph]] from the given location.
    */
  def load(path: String): PropertyGraph

  private[spark] def save(graph: PropertyGraph, path: String, saveMode: SaveMode): Unit

}
