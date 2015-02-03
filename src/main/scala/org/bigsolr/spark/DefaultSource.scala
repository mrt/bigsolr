package org.bigsolr.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider


class DefaultSource extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    SolrRelation(
      parameters("query"),
      parameters("serverUrl"),
      parameters("serverMode"),
      parameters("collection"),
      parameters("fields")
    )(sqlContext)
  }
  
}
