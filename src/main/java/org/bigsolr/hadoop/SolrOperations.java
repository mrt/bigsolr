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

import java.net.ConnectException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import org.apache.spark.api.java.function.*;
import org.apache.log4j.Logger;

public class SolrOperations {

  private static Logger log = Logger.getLogger(SolrOperations.class);

  public static final String SERVER_URL = "solr.server.url";
  private static final String SERVER_MODE = "solr.server.mode";
  private static final String COLLECTION_NAME = "solr.server.collection";
  private static final String FIELDS = "solr.server.fields";

  private static Map<String,SolrServer> solrServers = new HashMap<String, SolrServer>();
  private static final Map<String,CloudSolrServer> cachedServers = new HashMap<String,CloudSolrServer>();

  public static SolrServer getSolrServer(Configuration conf) {
    SolrServer solr = null;
    if(conf.get(SERVER_MODE).toLowerCase().equals("standalone")) {
      solr = getSolrHttpServer(conf.get(SERVER_URL));
    }
    else if(conf.get(SERVER_MODE).toLowerCase().equals("cloud")) {
      solr = getSolrCloudServer(conf.get(SERVER_URL), conf.get(COLLECTION_NAME));
    }
    else {
      log.error("This SERVER_MODE is not supported: " + conf.get(SERVER_MODE));
      System.exit(0);
    }
    return solr;
  }

  protected static HttpSolrServer getSolrHttpServer(String httpServerUrl) {
    // Better add authentication
    return new HttpSolrServer(httpServerUrl);
  }

  protected static CloudSolrServer getSolrCloudServer(String zkHostUrl, String collection) {
    CloudSolrServer cloudSolrServer = null;
    synchronized (cachedServers) {
      cloudSolrServer = cachedServers.get(zkHostUrl);
      if (cloudSolrServer == null) {
        cloudSolrServer = new CloudSolrServer(zkHostUrl);
        cloudSolrServer.setDefaultCollection(collection);
        cloudSolrServer.connect();
        cachedServers.put(zkHostUrl, cloudSolrServer);
      }
    }
    return cloudSolrServer;
  }

}
