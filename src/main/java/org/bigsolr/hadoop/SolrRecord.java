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

import java.util.Collection;
import java.util.Map;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.ObjectInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.hadoop.UnbufferedDataInputInputStream;
import org.apache.commons.lang.StringEscapeUtils;

import org.apache.log4j.Logger;

public class SolrRecord implements Writable, Serializable {

  private SolrDocument sd;

  private static Logger log = Logger.getLogger(SolrRecord.class);

  public SolrRecord() {
    log.debug("SolrRecord -> SolrRecord()");
  }

  public SolrRecord(SolrDocument doc) {
    log.debug("SolrRecord -> SolrRecord(SolrDocument sd)");
    this.sd = doc;
  }

  public SolrDocument getSolrDocument() {
    log.debug("SolrRecord -> getSolrDocument()");
    return this.sd;
  }

  public void setSolrDocument(SolrDocument doc) {
    log.debug("SolrRecord -> setSolrDocument()");
    this.sd = doc;
  }

  public Object getFirstValue(String name) {
    log.debug("SolrRecord -> getFirstValue()");
    return this.sd.getFirstValue(name);
  }

  public Object getFieldValue(String name) {
    log.debug("SolrRecord -> getFieldValue()");
    return this.sd.getFieldValue(name);
  }

  public Collection<Object> getFieldValues(String name) {
    log.debug("SolrRecord -> getFieldValues()");
    return this.sd.getFieldValues(name);
  }

  public Map<String,Collection<Object>> getFieldValuesMap() {
    log.debug("SolrRecord -> getFieldValuesMap() 1");
    return this.sd.getFieldValuesMap();
  }

  public Map<String,Object> getFieldValueMap() {
    log.debug("SolrRecord -> getFieldValuesMap() 2");
    return this.sd.getFieldValueMap();
  }

  public Collection<String> getFieldNames() {
    log.debug("SolrRecord -> getFieldNames()");
    return this.sd.getFieldNames();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    log.debug("SolrRecord -> write");
    
    JavaBinCodec codec = new JavaBinCodec();
    FastOutputStream os = FastOutputStream.wrap(DataOutputOutputStream.constructOutputStream(out));
    codec.init(os);
    try {
      codec.writeVal(sd);
    } finally {
      os.flushBuffer();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    log.debug("SolrRecord -> readFields");
    
    JavaBinCodec codec = new JavaBinCodec();
    UnbufferedDataInputInputStream is = new UnbufferedDataInputInputStream(in);
    sd = (SolrDocument)codec.readVal(is);
  }

  // Returns JSON string
  @Override
  public String toString() {
    log.debug("SolrRecord -> toString");

    StringBuffer jsonStr = new StringBuffer();
    jsonStr.append("{");
    for(String fieldName : sd.getFieldNames()) {
      String escapedFieldName = StringEscapeUtils.escapeJava((String)fieldName);
      String escapedFieldValue = StringEscapeUtils.escapeJava((String)sd.getFieldValue(fieldName).toString());
      jsonStr.append("\"" + escapedFieldName + "\":\"" + escapedFieldValue + "\",");
    }

    jsonStr = jsonStr.deleteCharAt(jsonStr.length() - 1); // remove the last ',' character
    jsonStr.append("}");

    return jsonStr.toString();
  }

}
