#BigSolr

BigSolr will try to provide comprehensive implementation of Solr connectors for Apache Hadoop, Spark and other big data technologies with Hadoop and Spark.

##Features


* Provides custom Hadoop APIs to access Solr servers
* Allows Apache Spark to read and write data with Solr servers through the Hadoop APIs
* Integration with Cascading/Scalding, Pig, Hive, etc. (future plan-- not supported yet)

## Requirements

* Apache Spark 1.1 or higher (1.2 is recommended)
* Apache Solr 4.10.x

## How to Build
<p>The following maven command creates <b>bigsolr-0.1.jar</b> in the target directory.</p>
```
$ mvn clean compile package
```


## How to Access Solr from Spark

### Prerequisites
<p>Before running the Spark program, Solr has to be up and running either in StandAlone or SolrCloud mode.</p>
```
In StandAlone/HttpServer mode:
$ bin/solr start

In SolrCloud (cloud) mode:
$ bin/solr -e cloud

```
For the following example, create collection/core (index table) named "collection1" once the Solr server is up and running. 


### Using Spark Shell
<p>Start Spark Shell</p>
<p><i>Note: please ensure your Spark distribution's Hadoop version! For Hadoop 2.3/2.4 or higher distributions, follow the instructions for New Hadoop API. If your Spark is Hadoop 1.x, please follow the instructions for old Hadoop API below.</i></p>
```
$ spark-shell --jars target/bigsolr-0.1.jar
```

### Reading with New Hadoop API (mapreduce)
```
scala> import org.apache.hadoop.conf.Configuration

scala> import org.apache.hadoop.io.NullWritable

scala> import org.bigsolr.hadoop.SolrInputFormat

scala> import org.bigsolr.hadoop.SolrRecord

----------------------------------------------
For SolrCloud mode

  scala> val serverMode: String = "cloud" 

  scala> val serverUrl: String = "localhost:9983"

For StandAlone/HttpServer mode

  scala> val serverMode: String = "standalone" 

  scala> val serverUrl: String = "http://localhost:8983/solr"

----------------------------------------------

scala> val collection: String = "collection1"

scala> val fields: String = "id,description"


scala> val queryStr: String = "description:*"

scala> var conf = new Configuration()

scala> conf.set("solr.query", queryStr)

scala> conf.set("solr.server.url", serverUrl)

scala> conf.set("solr.server.mode", serverMode)

scala> conf.set("solr.server.collection", collection)

scala> conf.set("solr.server.fields", fields)


scala> val rdds = sc.newAPIHadoopRDD(conf, 
					classOf[SolrInputFormat], 
					classOf[NullWritable], 
					classOf[SolrRecord]).map {
						case (key, value) => {
							value.getSolrDocument()
						}
					}

scala> rdds.count

scala> rdds.first

scala> rdds.first.getFieldValue("id")

scala> rdds.first.getFieldValue("description")

scala> rdds.first.getFieldValuesMap()


```

### Indexing with New Hadoop API (mapreduce)
```
scala> import org.bigsolr.hadoop.SolrOutputFormat
scala> import org.bigsolr.hadoop.SolrInputRecord

scala> import org.apache.hadoop.io.MapWritable
scala> import org.apache.hadoop.io.NullWritable
scala> import org.apache.hadoop.mapreduce.Job  // New Hadoop API
scala> import org.apache.hadoop.conf.Configuration
scala> import org.apache.hadoop.io.Text;

scala> var conf = new Configuration()
scala> conf.set("solr.server.url", serverUrl)
scala> conf.set("solr.server.mode", serverMode)
scala> conf.set("solr.server.collection", collection)
scala> conf.set("solr.server.fields", fields)

// Example with MapWritable

scala> val m1 = Map("id" -> "1", "description" -> "apple orange New York", "author" -> scala> val m2 = Map("id" -> "2", "description" -> "apple peach San Diego", "author" -> "Kevin")
scala> val m3 = Map("id" -> "3", "description" -> "Apple tomato San Francisco", "author" -> "Nick")
scala> val l1 = List[Map[String,String]](m1,m2,m3)
scala> val rdds1 = sc.parallelize(l1)

scala> val rdds1a = rdds1.map(e => {
	  val record = new MapWritable()
  	  val id = e.getOrElse("id", "")
  	  val description = e.getOrElse("description", "")
      val author = e.getOrElse("author", "")
  	  record.put(new Text("id"), new Text(id))
  	  record.put(new Text("description"), new Text(description))
      record.put(new Text("author"), new Text(author))
  	  (NullWritable.get, record)
	})

// Index with MapWritable
scala> rdds1a.saveAsNewAPIHadoopFile(
  	  "-", // this path parameter will be ignored
  	  classOf[NullWritable],
  	  classOf[MapWritable],
  	  classOf[SolrOutputFormat],
  	  conf
    )


// Example with SolrInputRecord (Wrapper for SolrInputDocument)

scala> val m4 = Map("id" -> "4", "description" -> "orange lake Florida", "author" -> "Emily")
scala> val m5 = Map("id" -> "5", "description" -> "cherry forest Vermont", "author" -> "Kate")
scala> val m6 = Map("id" -> "6", "description" -> "strawberry beach California", "author" -> "Monica")
scala> val l2 = List[Map[String,String]](m4,m5,m6)
scala> val rdds2 = sc.parallelize(l2)

scala> val rdds2a = rdds2.map(e => {
      val record = new SolrInputRecord()
  	  val id = e.getOrElse("id", "")
  	  val description = e.getOrElse("description", "")
  	  val author = e.getOrElse("author", "")
      record.setField("id", id)
      record.setField("description", description)
      record.setField("author", author)
  	  //record.put(new Text(id), new Text(description))
  	  (NullWritable.get, record)
	  })

// Index with SolrInputRecord
scala> rdds2a.saveAsNewAPIHadoopFile(
  	  "-",
  	  classOf[NullWritable],
  	  classOf[SolrInputRecord],
  	  classOf[SolrOutputFormat],
  	  conf
    )

```


### Reading with old Hadoop API (mapred)</p>
```
scala> import org.apache.hadoop.mapred.JobConf

scala> import org.apache.hadoop.io.NullWritable

scala> import org.bigsolr.hadoop.SolrInputFormat

scala> import org.bigsolr.hadoop.SolrRecord

----------------------------------------------
For SolrCloud mode

  scala> val serverMode: String = "cloud" 

  scala> val serverUrl: String = "localhost:9983"

For StandAlone/HttpServer mode

  scala> val serverMode: String = "standalone" 

  scala> val serverUrl: String = "http://localhost:8983/solr"

----------------------------------------------

scala> val collection: String = "collection1"

scala> val fields: String = "id,description"

scala> val queryStr: String = "description:*"

scala> var conf = new JobConf(sc.hadoopConfiguration)

scala> conf.set("solr.query", queryStr)

scala> conf.set("solr.server.url", serverUrl)

scala> conf.set("solr.server.mode", serverMode)

scala> conf.set("solr.server.collection", collection)

scala> conf.set("solr.server.fields", fields)


scala> val rdds = sc.hadoopRDD(conf, 
					classOf[SolrInputFormat], 
					classOf[NullWritable], 
					classOf[SolrRecord]).map {
						case (key, value) => {
							value.getSolrDocument()
						}
					}

scala> rdds.count

scala> rdds.first

scala> rdds.first.getFieldValue("id")

scala> rdds.first.getFieldValue("description")

scala> rdds.first.getFieldValuesMap()

```

### Indexing with Old Hadoop API (mapred)
```
scala> import org.bigsolr.hadoop.SolrOutputFormat
scala> import org.bigsolr.hadoop.SolrInputRecord

scala> import org.apache.hadoop.io.MapWritable
scala> import org.apache.hadoop.io.NullWritable
scala> import org.apache.hadoop.mapred.JobConf  // Old Hadoop API
scala> import org.apache.hadoop.conf.Configuration
scala> import org.apache.hadoop.io.Text;

scala> var conf = new JobConf(sc.hadoopConfiguration)
scala> conf.set("solr.server.url", serverUrl)
scala> conf.set("solr.server.mode", serverMode)
scala> conf.set("solr.server.collection", collection)
scala> conf.set("solr.server.fields", fields)

scala> val m1 = Map("id" -> "1", "description" -> "apple orange New York", "author" -> scala> val m2 = Map("id" -> "2", "description" -> "apple peach San Diego", "author" -> "Kevin")
scala> val m3 = Map("id" -> "3", "description" -> "Apple tomato San Francisco", "author" -> "Nick")
scala> val l1 = List[Map[String,String]](m1,m2,m3)
scala> val rdds1 = sc.parallelize(l1)

// Example with MapWritable
scala> val rdds1a = rdds1.map(e => {
	  val record = new MapWritable()
  	  val id = e.getOrElse("id", "")
  	  val description = e.getOrElse("description", "")
      val author = e.getOrElse("author", "")
  	  record.put(new Text("id"), new Text(id))
  	  record.put(new Text("description"), new Text(description))
      record.put(new Text("author"), new Text(author))
  	  (NullWritable.get, record)
	})

// Index with MapWritable
scala> rdds1a.saveAsHadoopFile(
  	  "-",  // No Path-- will be ignored
  	  classOf[NullWritable],
  	  classOf[MapWritable],
  	  classOf[SolrOutputFormat],
  	  conf,
      None
    )

scala> val m4 = Map("id" -> "4", "description" -> "orange lake Florida", "author" -> "Emily")
scala> val m5 = Map("id" -> "5", "description" -> "cherry forest Vermont", "author" -> "Kate")
scala> val m6 = Map("id" -> "6", "description" -> "strawberry beach California", "author" -> "Monica")
scala> val l2 = List[Map[String,String]](m4,m5,m6)
scala> val rdds2 = sc.parallelize(l2)

// Example with SolrInputRecord (Wrapper for SolrInputDocument)
scala> val rdds2a = rdds2.map(e => {
      val record = new SolrInputRecord()
  	  val id = e.getOrElse("id", "")
  	  val description = e.getOrElse("description", "")
  	  val author = e.getOrElse("author", "")
      record.setField("id", id)
      record.setField("description", description)
      record.setField("author", author)
  	  //record.put(new Text(id), new Text(description))
  	  (NullWritable.get, record)
	  })

// Index with SolrInputRecord
scala> rdds1a.saveAsHadoopFile(
  	  "-",  // No Path-- will be ignored
  	  classOf[NullWritable],
  	  classOf[MapWritable],
  	  classOf[SolrOutputFormat],
  	  conf,
      None
    )
```



## License
This software is available under the [Apache License, Version 2.0](LICENSE.txt).    

## Reporting Bugs
Please use GitHub to report feature requests or bugs.  