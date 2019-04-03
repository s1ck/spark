package org.apache.spark.graph

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BinaryType, ByteType, IntegerType, LongType, MetadataBuilder, ShortType, StringType}

package object api {

  val idColumnName = "$ID"

  val sourceIdColumnName = "$SOURCE_ID"
  val targetIdColumnName = "$TARGET_ID"

  val labelsMetadataKey = "labels"
  val relationshipTypeMetadataKey = "relationshipType"

  implicit class GraphElementFrameOps(val df: DataFrame) extends AnyVal {

    def toNodeFrame(labels: String*): DataFrame = toNodeFrame(labels.toSet)

    def toNodeFrame(labels: Set[String]): DataFrame = df
      .withEncodedIdColumns(idColumnName)
      .withLabels(labels)

    def isNodeFrame: Boolean = df.columns.contains(idColumnName) && df.schema(idColumnName).metadata.contains(labelsMetadataKey)

    def labels: Set[String] = df.schema(idColumnName).metadata.getStringArray(labelsMetadataKey).toSet

    def nodeProperties: Set[String] = df.columns.toSet - idColumnName

    def toRelationshipFrame(relationshipType: String): DataFrame = df
      .withEncodedIdColumns(idColumnName, sourceIdColumnName, targetIdColumnName)
      .withRelationshipType(relationshipType)

    def isRelationshipFrame: Boolean = df.columns.contains(idColumnName) && df.schema(idColumnName).metadata.contains(relationshipTypeMetadataKey)

    def relationshipType: String = df.schema(idColumnName).metadata.getString(relationshipTypeMetadataKey)

    def relationshipProperties: Set[String] = df.columns.toSet - idColumnName - sourceIdColumnName - targetIdColumnName

    def withLabels(labels: String*): DataFrame = withLabels(labels.toSet)

    def withLabels(labels: Set[String]): DataFrame = {
      val labelSetMetadata = new MetadataBuilder()
        .putStringArray(labelsMetadataKey, labels.toArray.sorted)
        .build()
      val newColumn = df.col(idColumnName).as(idColumnName, labelSetMetadata)
      df.withColumn(idColumnName, newColumn)
    }

    def withRelationshipType(relationshipType: String): DataFrame = {
      val relationshipTypeMetadata = new MetadataBuilder()
        .putString(relationshipTypeMetadataKey, relationshipType)
        .build()
      val newColumn = df.col(idColumnName).as(idColumnName, relationshipTypeMetadata)
      df.withColumn(idColumnName, newColumn)
    }

    def withEncodedIdColumns(idColumnNames: String*): DataFrame = {
      val encodedIdCols = idColumnNames.map { idColumnName =>
        val col = df.col(idColumnName)
        df.schema(idColumnName).dataType match {
          case BinaryType => col
          case StringType | ByteType | ShortType | IntegerType | LongType => col.cast(BinaryType)
          // TODO: Constrain to types that make sense as IDs
          case _ => col.cast(StringType).cast(BinaryType)
        }
      }
      val remainingColumnNames = df.columns.filterNot(idColumnNames.contains)
      val remainingCols = remainingColumnNames.map(df.col)
      df.select(encodedIdCols ++ remainingCols: _*)
    }
  }
}
