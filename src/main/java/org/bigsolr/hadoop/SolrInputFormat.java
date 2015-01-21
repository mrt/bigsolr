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
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.client.solrj.response.QueryResponse;

import org.bigsolr.hadoop.SolrOperations;

import org.apache.log4j.Logger;

public class SolrInputFormat extends InputFormat<NullWritable, SolrRecord> 
  implements org.apache.hadoop.mapred.InputFormat<NullWritable, SolrRecord> {

	private static Logger log = Logger.getLogger(SolrInputFormat.class);
    
  private static final String SOLR_QUERY = "solr.query";
  private static final String SERVER_MODE = "solr.server.mode";
  private static final String SERVER_URL = "solr.server.url";
  private static final String COLLECTION_NAME = "solr.server.collection";
  private static final String FIELDS = "solr.server.fields";
  private static final String ID_FIELD = "id";

  // New API
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    log.info("SolrInputFormat -> getSplits");

    Configuration conf = context.getConfiguration();
    String collectionName = conf.get(COLLECTION_NAME);
    int numSplits = context.getNumReduceTasks();
    SolrServer solr = SolrOperations.getSolrServer(conf);

    final SolrQuery solrQuery = new SolrQuery(conf.get(SOLR_QUERY));
    solrQuery.setFields(ID_FIELD);
    solrQuery.setRows(50);
    solrQuery.set("collection", collectionName);
    solrQuery.setStart(0);

    QueryResponse response;
    try {
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw new IOException(e);
    }

    int numResults = (int)response.getResults().getNumFound();
    int numDocsPerSplit = (numResults / numSplits); 
    int currentDoc = 0;

    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < numSplits - 1; i++) {
      splits.add(new SolrInputSplit(currentDoc, numDocsPerSplit));
      currentDoc += numDocsPerSplit;
    }
    splits.add(new SolrInputSplit(currentDoc, numResults - currentDoc));

    return splits;
  }

  // Old API
  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(org.apache.hadoop.mapred.JobConf conf, int numSplits) throws IOException {
    log.info("SolrInputFormat -> getSplits");
    String collectionName = conf.get(COLLECTION_NAME);
    SolrServer solr = SolrOperations.getSolrServer(conf);

    final SolrQuery solrQuery = new SolrQuery(conf.get(SOLR_QUERY));
    solrQuery.setFields(ID_FIELD);
    solrQuery.setRows(50);
    solrQuery.set("collection", collectionName);
    solrQuery.setStart(0);

    QueryResponse response;
    try {
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw new IOException(e);
    }

    int numResults = (int)response.getResults().getNumFound();
    int numDocsPerSplit = (numResults / numSplits); 
    int currentDoc = 0;

    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < numSplits - 1; i++) {
      splits.add(new SolrInputSplit(currentDoc, numDocsPerSplit));
      currentDoc += numDocsPerSplit;
    }
    splits.add(new SolrInputSplit(currentDoc, numResults - currentDoc));

    return splits.toArray(new SolrInputSplit[splits.size()]);
  }


  // New API
  @Override
  public RecordReader<NullWritable, SolrRecord> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    
    log.info("SolrInputFormat -> createRecordReader");

    Configuration conf = context.getConfiguration();
    org.apache.hadoop.mapred.Reporter reporter = null;  // Need to implement with heartbeat
    
    String collectionName = conf.get(COLLECTION_NAME);
    String fields = conf.get(FIELDS);
    SolrServer solr = SolrOperations.getSolrServer(conf);

    SolrInputSplit solrSplit = (SolrInputSplit) split;
    final int numDocs = (int) solrSplit.getLength();
      
    final SolrQuery solrQuery = new SolrQuery(conf.get(SOLR_QUERY));

    solrQuery.setFields(fields);
    solrQuery.set("collection", collectionName);
    solrQuery.setStart(solrSplit.getDocBegin());
    solrQuery.setRows(numDocs);

    QueryResponse response;
    try {
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw new IOException(e);
    }

    final SolrDocumentList solrDocs = response.getResults();
    return new SolrRecordReader(solrDocs, numDocs);
  }

  // Old API
  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, SolrRecord> getRecordReader(org.apache.hadoop.mapred.InputSplit split, 
      org.apache.hadoop.mapred.JobConf conf, org.apache.hadoop.mapred.Reporter reporter) throws IOException {

    log.info("SolrInputFormat -> getRecordReader");

    String collectionName = conf.get(COLLECTION_NAME);
    String fields = conf.get(FIELDS);
    SolrServer solr = SolrOperations.getSolrServer(conf);
    int numDocs = 0;

    SolrInputSplit solrSplit = (SolrInputSplit) split;
    try {
      numDocs = (int) solrSplit.getLength();
    } catch (final IOException e) {
      throw new IOException(e);
    }
      
    final SolrQuery solrQuery = new SolrQuery(conf.get(SOLR_QUERY));
    solrQuery.setFields(fields);
    solrQuery.set("collection", collectionName); // Added
    solrQuery.setStart(solrSplit.getDocBegin());
    solrQuery.setRows(numDocs);

    QueryResponse response = null;
    try {
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw new IOException(e);
    }

    final SolrDocumentList solrDocs = response.getResults();
    return new SolrRecordReader(solrDocs, numDocs);
  }

}


