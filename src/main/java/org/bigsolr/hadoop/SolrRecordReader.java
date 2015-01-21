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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import org.bigsolr.hadoop.SolrRecord;

import org.apache.log4j.Logger;

public class SolrRecordReader extends RecordReader<NullWritable, SolrRecord> 
    implements org.apache.hadoop.mapred.RecordReader<NullWritable, SolrRecord>{

    private int currentDoc = 0;
    private int numDocs;
    private SolrRecord record;
    private SolrDocumentList solrDocs;

    private static Logger log = Logger.getLogger(SolrRecordReader.class);
    
    // New API
    public SolrRecordReader(SolrDocumentList solrDocs, int numDocs) {
      this.solrDocs = solrDocs;
      this.numDocs = numDocs;
    }
    
    // New API
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      record = new SolrRecord();   
    }

    // New & Old API
    @Override
    public void close() throws IOException { }

    // New & Old API
    @Override
    public float getProgress() throws IOException {
      return currentDoc / (float) numDocs;
    }

    // New API
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    // New API
    @Override
    public SolrRecord getCurrentValue() throws IOException, InterruptedException {
      return record;
    }

    // New API
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (currentDoc >= numDocs) {
        return false;
      }

      SolrDocument doc = solrDocs.get(currentDoc);
      record.setSolrDocument(doc);
      currentDoc++;
      return true;
    }

    // Old API (mapred)
    @Override
    public boolean next(NullWritable key, SolrRecord value) throws IOException {
      if (currentDoc >= numDocs) {
        return false;
      }

      SolrDocument doc = solrDocs.get(currentDoc);
      value.setSolrDocument(doc);
      currentDoc++;
      return true;
    }

    // Old API (mapred)
    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    // Old API (mapred)
    @Override
    public SolrRecord createValue() {
      return new SolrRecord();
    }

    // Old API (mapred)
    @Override
    public long getPos() {
      return currentDoc;
    }
   
  }


