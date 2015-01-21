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

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputCommitter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import org.apache.log4j.Logger;

public class SolrOutputFormat extends OutputFormat 
  implements org.apache.hadoop.mapred.OutputFormat {

  private static Logger log = Logger.getLogger(SolrOutputFormat.class);

  // New API
  @Override
  public SolrRecordWriter getRecordWriter(TaskAttemptContext context) 
  		throws IOException, InterruptedException {
  	
  	log.info("SolrOutputFormat -> getRecordWriter (new API)");
  	return new SolrRecordWriter(context);
  }

  // Old API
  @Override
  public SolrRecordWriter getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    
    log.info("SolrOutputFormat -> getRecordWriter (old API)");
    return new SolrRecordWriter(job);
  }

  // New API
  @Override
  public void checkOutputSpecs(JobContext context) 
  		throws IOException, InterruptedException {

  	log.info("SolrOutputFormat -> checkOutputSpecs (New API)");
  	// none
  }

  // Old API
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    log.info("SolrOutputFormat -> checkOutputSpecs (Old API)");
    // none
  }

  // New API
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  		throws IOException, InterruptedException {

  	log.info("SolrOutputFormat -> getOutputCommitter");
  	//return an empty outputcommitter
    return new OutputCommitter() {
      @Override
      public void setupTask(TaskAttemptContext arg0) throws IOException {
      }
      @Override
      public void setupJob(JobContext arg0) throws IOException {
      }
      @Override
      public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
        return false;
      }
      @Override
      public void commitTask(TaskAttemptContext arg0) throws IOException {
      }
      @Override
      public void abortTask(TaskAttemptContext arg0) throws IOException {
      }
    };
  }


}