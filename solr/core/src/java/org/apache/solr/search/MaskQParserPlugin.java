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

import java.util.HashMap;

/**
 * {mask m=4}q Create a query "q" trace with bitset '4'
 */
public class MaskQParserPlugin extends QParserPlugin {
  public static String NAME="tag";
  public static String T="t";  // SPecific a string tag that will be allocated as a bit on a bitmask


  // Associate a tag with a mask
  public static class MaskTagMap {
    int max_bit;
    String[] tags = new String[64];
    HashMap<String, Integer> bits = new HashMap<String, Integer>();

    public long getBitmask(String tag) {
      if (bits.containsKey(tag)) {
         Integer b = bits.get(tag);
        return (1L << b);
      } else {
        bits.put(tag, max_bit);
        tags[max_bit] = tag;
        max_bit ++;
        return (1L << (max_bit-1));
      }
    }

    public String getTag(int bit) {
      String s = tags[bit];
      return s == null ? "bit" + Integer.toString(bit) : s;
    }

    public static MaskTagMap get(SolrQueryRequest req) {
        Object o = req.getContext().get("MASKTAGMAP");
      if (o == null) {
          MaskTagMap m = new MaskTagMap();
        req.getContext().put("MASKTAGMAP", m);
        return m;
      } else {
          return (MaskTagMap) o;
      }
    }


  }

  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      QParser baseParser;
      ValueSource vs;
      String m;
      String t;

      @Override
      public Query parse() throws SyntaxError {
        t = localParams.get(T, null);
        baseParser = subQuery(localParams.get(QueryParsing.V), null);
        Query q = baseParser.getQuery();


        long bitmask;
        try {
          bitmask = Long.parseLong(t);
        } catch (NumberFormatException e) {
          bitmask = MaskTagMap.get(req).getBitmask(t);
        }
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
