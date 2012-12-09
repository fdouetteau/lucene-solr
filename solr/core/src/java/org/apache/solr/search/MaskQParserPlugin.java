package org.apache.solr.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.MaskQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * {mask m=4}q Create a query "q" trace with bitset '4'
 */
public class MaskQParserPlugin extends QParserPlugin {
  public static String NAME="mask";
  public static String M="m";

  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      QParser baseParser;
      ValueSource vs;
      String m;

      @Override
      public Query parse() throws SyntaxError {
        m = localParams.get(M);
        baseParser = subQuery(localParams.get(QueryParsing.V), null);
        Query q = baseParser.getQuery();

        if (m == null) return q;
        long bitmask = Long.parseLong(m);
        return new MaskQuery(q, bitmask);
      }


      @Override
      public String[] getDefaultHighlightFields() {
        return baseParser.getDefaultHighlightFields();
      }

      @Override
      public Query getHighlightQuery() throws SyntaxError {
        return baseParser.getHighlightQuery();
      }

      @Override
      public void addDebugInfo(NamedList<Object> debugInfo) {
        // encapsulate base debug info in a sub-list?
        baseParser.addDebugInfo(debugInfo);
        debugInfo.add("mask",m);
      }
    };
  }





}
