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
 * A Query that activates a bitmask to track the fact that a doc matched a subquery in the query tree.
 */
public class MaskQuery extends Query {

  // Subquery
  Query query;

  // Bitmask to apply when matching
  long bitmask;

  public MaskQuery(Query query, long bitmask) {
    this.query = query;
    this.bitmask = bitmask;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (query != null) {
      Query rewritten = query.rewrite(reader);
      if (rewritten != query) {
        rewritten = new MaskQuery(rewritten, bitmask);
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


  @Override
  public String toString(String field) {
    return new StringBuilder()
        .append("{")
        .append(query.toString(field))
        .append(",mask=")
        .append(bitmask)
        .append('}')
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!super.equals(o))
      return false;
    if (o instanceof MaskQuery) {
      final MaskQuery other = (MaskQuery) o;
      return this.bitmask == other.bitmask &&
              ((this.query == null) ? other.query == null : this.query.equals(other.query));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() +
        query.hashCode() + Long.valueOf(bitmask).hashCode();
  }



  protected class MaskWeight extends Weight {
    private final Weight innerWeight;
    private final long bitmask;

    public MaskWeight(IndexSearcher searcher, long bitmask) throws IOException {
      this.innerWeight = query.createWeight(searcher);
      this.bitmask = bitmask;
    }

    @Override
    public Query getQuery() {
      return MaskQuery.this;
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
      return new TaggedScorer(innerScorer, this, bitmask);
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
    final long bitmask;

    public TaggedScorer(Scorer innerScorer, Weight w, long bitmask) {
      super(w);
      this.innerScorer = innerScorer;
      this.bitmask = bitmask;
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
      return innerScorer.score();
    }

    public long bitmask() {
      return bitmask;
    }

    @Override
    public int freq() throws IOException {
      return innerScorer.freq();
    }

    @Override
    public int advance(int target) throws IOException {
      return innerScorer.advance(target);
    }

    @Override
    public void score(Collector collector) throws IOException {
      super.score(collector);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean score(Collector collector, int max, int firstDocID) throws IOException {
      return super.score(collector, max, firstDocID);    //To change body of overridden methods use File | Settings | File Templates.
    }
  }
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new MaskWeight(searcher, bitmask);
  }

}
