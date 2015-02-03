package org.bigsolr.spark

import org.bigsolr.hadoop.SolrInputFormat
import org.bigsolr.hadoop.SolrRecord
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._
import scala.language.existentials

import org.apache.spark.sql._
import org.apache.spark.sql.sources.{TableScan, PrunedFilteredScan, BaseRelation, Filter}


case class SolrRelation(
              query: String,
              serverUrl: String,
              serverMode: String,
              collection: String,
              fields: String
            )(@transient val sqlContext: SQLContext) extends PrunedFilteredScan {

  val schema = {
  	StructType(fields.split(",").map(fieldName => StructField(fieldName, StringType, true)))
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
  	
    // Build the job configuration
    var conf = new Configuration()
    conf.set("solr.query", query)
    conf.set("solr.server.url", serverUrl)
    conf.set("solr.server.mode", serverMode)
    conf.set("solr.server.collection", collection)
    conf.set("solr.server.fields", fields)
    
    val rdds = sqlContext.sparkContext.newAPIHadoopRDD(
    	conf,
    	classOf[SolrInputFormat],
    	classOf[NullWritable],
    	classOf[SolrRecord]
    	)

    rdds.map {
      case (key, value) => {
        val row = scala.collection.mutable.ListBuffer.empty[String]
        requiredColumns.foreach{field =>
          row += value.getFieldValues(field).toString()
        }

        Row.fromSeq(row)
      }
    }

  }

}