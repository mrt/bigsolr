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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.log4j.Logger;

public class SolrInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

    private int docBegin;
    private int numDocs;
    private static Logger log = Logger.getLogger(SolrInputSplit.class);

    public SolrInputSplit() { }

    public SolrInputSplit(int docBegin, int numDocs) {
      this.docBegin = docBegin;
      this.numDocs = numDocs;
    }

    public int getDocBegin() {
      return docBegin;
    }

    @Override
    public long getLength() throws IOException {
      return numDocs;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[] {} ;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      docBegin = in.readInt();
      numDocs = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(docBegin);
      out.writeInt(numDocs);
    } 
}
