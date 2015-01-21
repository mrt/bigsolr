/*
 * Licensed to Taka Shinagawa under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.bigsolr.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;

import org.bigsolr.hadoop.SolrOperations;

import org.apache.log4j.Logger;

import org.apache.hadoop.mapred.JobConf;

public class SolrRecordWriter extends RecordWriter<NullWritable, Writable> 
    implements org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable> {

  private static final String SERVER_URL = "solr.server.url";
  private static final String SERVER_MODE = "solr.server.mode";
  private static final String COLLECTION_NAME = "solr.server.collection";
  private static final String FIELDS = "solr.server.fields";

  private SolrServer solr;
  private Configuration conf;
  private static Logger log = Logger.getLogger(SolrRecordWriter.class);

  // For New API
  public SolrRecordWriter(TaskAttemptContext context){

  	log.info("SolrRecordWriter ->  SolrRecordWriter(TaskAttemptContext context)");
  	conf = context.getConfiguration();
    solr = SolrOperations.getSolrServer(conf);
  }

  // For Old API
  public SolrRecordWriter(JobConf conf){

    log.info("SolrRecordWriter ->  SolrRecordWriter(JobConf conf)");
    solr = SolrOperations.getSolrServer(conf);
  }

  // New & Old API
  @Override
  public void write(NullWritable key, Writable value) throws IOException {
  	
    log.info("SolrRecordWriter ->  write");

  	if(solr == null) {
      solr = SolrOperations.getSolrServer(conf);
  	}

    SolrInputDocument doc = new SolrInputDocument();
    if(value.getClass().getName().equals("org.apache.hadoop.io.MapWritable")){
      MapWritable valueMap = (MapWritable) value;

      for(Writable keyWritable : valueMap.keySet()) {
        String fieldName = keyWritable.toString();
        Object fieldValue = valueMap.get(new Text(fieldName));
        // Need to add proper conversion of object to Schema field type
        doc.addField(fieldName, fieldValue.toString());
      }
    }
    else if(value.getClass().getName().equals("org.bigsolr.hadoop.SolrInputRecord")) {
      doc = (SolrInputDocument) value;
    }
    else {
      log.error("SolrRecordWriter write() Class for Value is not Supported: " + value.getClass().getName());
      System.exit(0);
    }
  	
  	try {
  	  solr.add(doc);
  	  //solr.commit(true,true);
  	} catch (SolrServerException e) {
  	  log.error("SolrRecordWriter-- solr.add(doc) failed");
      throw new IOException(e);
  	}
  	
  }

  // New API
  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
  	log.info("SolrRecordWriter -> close (new API)");
  	
  	try {
  	  solr.commit();
  	} catch (SolrServerException e) {
  	  log.error("SolrRecordWriter-- solr.commit() failed");
      throw new IOException(e);
  	}

  	solr.shutdown();
  }

  // Old API
  @Override
  public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException {
    log.info("SolrRecordWriter -> close (old API)");
    try {
      solr.commit();
    } catch (SolrServerException e) {
      log.error("SolrRecordWriter-- solr.commit() failed");
      throw new IOException(e);
    }

    solr.shutdown();
  }

}