package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;

/**
 * Query that tags an existing query and issue
 */
public class TaggedQuery extends Query {

  // Subquery
  Query query;

  // Tag to issue.
  String tag;

  public TaggedQuery(Query query, String tag) {
    this.query = query;
    this.tag = tag;

  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (query != null) {
      Query rewritten = query.rewrite(reader);
      if (rewritten != query) {
        rewritten = new TaggedQuery(rewritten, tag);
        return rewritten;
      }
    }
    return this;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    if (query != null)
      query.extractTerms(terms);
  }

  protected class TaggedWeight extends Weight {
    private final Weight innerWeight;
    private final String tag;

    public TaggedWeight(IndexSearcher searcher, String tag) throws IOException {
      this.innerWeight = query.createWeight(searcher);
      this.tag = tag;
    }

    @Override
    public Query getQuery() {
      return TaggedQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      // we calculate sumOfSquaredWeights of the inner weight, but ignore it (just to initialize everything)
      return innerWeight.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      innerWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
                         boolean topScorer, final Bits acceptDocs) throws IOException {
      Scorer innerScorer = innerWeight.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      if (innerScorer == null) {
        return null;
      }
      return new TaggedScorer(innerScorer, this, tag);
    }

    @Override
    public boolean scoresDocsOutOfOrder() {
      return innerWeight.scoresDocsOutOfOrder();
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      return innerWeight.explain(context, doc);
    }
  }

  protected class TaggedScorer extends Scorer {
    final Scorer innerScorer;
    OutOfOrderTopTaggedScoreDocCollector collector;
    final String tag;

    public TaggedScorer(Scorer innerScorer, Weight w, String tag) {
      super(w);
      this.innerScorer = innerScorer;
      this.tag = tag;
    }

    @Override
    public int nextDoc() throws IOException {
      return innerScorer.nextDoc();
    }

    @Override
    public int docID() {
      return innerScorer.docID();
    }

    @Override
    public float score() throws IOException {
      assert innerScorer.docID() != NO_MORE_DOCS;

      if (collector != null) {
          collector.addTag(tag);
      }
      return innerScorer.score();
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int advance(int target) throws IOException {
      return innerScorer.advance(target);
    }

    @Override
    public void score(Collector collector) throws IOException {
      if (collector instanceof OutOfOrderTopTaggedScoreDocCollector) {
        this.collector = (OutOfOrderTopTaggedScoreDocCollector) collector;
      }
      super.score(collector);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean score(Collector collector, int max, int firstDocID) throws IOException {
      if (collector instanceof OutOfOrderTopTaggedScoreDocCollector) {
        this.collector = (OutOfOrderTopTaggedScoreDocCollector) collector;
      }
      return super.score(collector, max, firstDocID);    //To change body of overridden methods use File | Settings | File Templates.
    }
  }
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new TaggedWeight(searcher, tag);
  }

  @Override
  public String toString(String field) {
    return new StringBuilder("TaggedQuery(")
        .append(query.toString(field))
        .append(')')
        .append(tag)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!super.equals(o))
      return false;
    if (o instanceof TaggedQuery) {
      final TaggedQuery other = (TaggedQuery) o;
      return
          ((this.tag == null) ? other.tag == null : this.tag.equals(other.tag)) &&
              ((this.query == null) ? other.query == null : this.query.equals(other.query));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() +
        query.hashCode() + tag.hashCode();
  }




}
