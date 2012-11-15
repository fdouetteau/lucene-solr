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

/**
 * A Score Doc with additional tags.
 */
public class TaggedScoreDoc extends ScoreDoc  {

  public String tag;

  /** Constructs a ScoreDoc. */
  public TaggedScoreDoc(int doc, float score, String tag) {
    super(doc, score);
    this.tag = tag;
  }

  /** Constructs a ScoreDoc. */
  public TaggedScoreDoc(int doc, float score, int shardIndex, String tag) {
    super(doc, score, shardIndex);
    this.tag = tag;
  }

  @Override
  public String toString() {
    return super.toString() + " tag="  + tag;
  }
}
