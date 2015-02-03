package org.bigsolr.spark

import org.apache.spark.sql.{SQLContext, SchemaRDD}

package object solr {

  implicit class SolrContext(sqlContext: SQLContext) {

    def query(queryStr:String, 
              serverUrlStr:String, 
              serverModeStr:String,
              collectionStr:String,
              fieldsStr:String) = {

      val solrRelation = SolrRelation(
                            query = queryStr,
                            serverUrl = serverUrlStr,
                            serverMode = serverModeStr,
                            collection = collectionStr,
                            fields = fieldsStr
                          )(sqlContext)
      sqlContext.baseRelationToSchemaRDD(solrRelation)

    }
    
    def writeToIndex(filePath: String) = {
      // To-do

    }

  }


  implicit class SolrSchemaRDD(schemaRDD: SchemaRDD) {

    def saveInIndex(path: String): Unit = {
      // To-do
      
    }
  }
}
