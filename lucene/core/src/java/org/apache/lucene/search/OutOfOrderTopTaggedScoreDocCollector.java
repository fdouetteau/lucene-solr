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

import java.io.IOException;

/**
 * Collect and score TaggedScoreDoc assuming out of order
 */
public class OutOfOrderTopTaggedScoreDocCollector extends TopDocsCollector<TaggedScoreDoc> {
  int numHits;
  TaggedScoreDoc pqTop;
  int docBase = 0;
  Scorer scorer;

  public OutOfOrderTopTaggedScoreDocCollector(int numHits) {
    super(new TaggedHitQueue(numHits, true));
    this.numHits = numHits;
    this.pqTop = pq.top();
  }
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }

  // Tag for the current document.
  protected String docTag;

  /**
   * Callback from scorer.score() to add a tag for the current doc;
   * @param tag
   */
  public void addTag(String tag) {
    docTag = docTag + "," + tag; // TODO: use a StringBuilder for performance.
  }

  @Override
  public void collect(int doc) throws IOException {
    docTag = "";
    float score = scorer.score();

    // This collector cannot handle NaN
    assert !Float.isNaN(score);

    totalHits++;
    if (score < pqTop.score) {
      // Doesn't compete w/ bottom entry in queue
      return;
    }
    doc += docBase;
    if (score == pqTop.score && doc > pqTop.doc) {
      // Break tie in score by doc ID:
      return;
    }
    pqTop.doc = doc;
    pqTop.score = score;
    pqTop.tag = docTag;
    pqTop = pq.updateTop();
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    docBase = context.docBase;
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }
}
