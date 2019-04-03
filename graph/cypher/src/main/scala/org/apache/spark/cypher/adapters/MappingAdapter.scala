package org.apache.spark.cypher.adapters

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.apache.spark.graph.api._

object MappingAdapter {

  implicit class RichNodeDataFrame(val nodeDf: DataFrame) extends AnyVal {
    def toNodeMapping: EntityMapping = NodeMappingBuilder
      .on(idColumnName)
      .withImpliedLabels(nodeDf.labels.toSeq: _*)
      .withPropertyKeys(nodeDf.relationshipProperties.toSeq:_*)
      .build
  }

  implicit class RichRelationshipDataFrame(val relDf: DataFrame) extends AnyVal {
    def toRelationshipMapping: EntityMapping = RelationshipMappingBuilder
        .on(idColumnName)
        .withSourceStartNodeKey(sourceIdColumnName)
        .withSourceEndNodeKey(targetIdColumnName)
        .withRelType(relDf.relationshipType)
        .withPropertyKeys(relDf.relationshipProperties.toSeq: _*)
        .build
  }
}
