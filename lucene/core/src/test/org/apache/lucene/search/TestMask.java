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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Similarity unit test.
 *
 *
 */
public class TestMask extends LuceneTestCase {

  public void testMask() throws Exception {
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), store);

    Document d1 = new Document();
    d1.add(newTextField("field", "a b", Field.Store.YES));
    Document d2 = new Document();

    d2.add(newTextField("field", "a c d", Field.Store.YES));

    Document d3 = new Document();
    d3.add(newTextField("field", "a c", Field.Store.YES));


    writer.addDocument(d1);
    writer.addDocument(d2);
    writer.addDocument(d3);

    IndexReader reader = writer.getReader();

    IndexSearcher searcher = newSearcher(reader);

    BooleanQuery query = new BooleanQuery();
    query.add(new MaskQuery(new TermQuery(new Term("field", "a")), 2), BooleanClause.Occur.SHOULD);
    query.add(new MaskQuery(new TermQuery(new Term("field", "d")), 4), BooleanClause.Occur.SHOULD);
    query.add(new MaskQuery(new TermQuery(new Term("field", "b")), 1), BooleanClause.Occur.MUST_NOT);
    MaskQuery query2  = new MaskQuery(query, 4);

    System.out.println(query2.toString());
    System.out.println(query2.createWeight(searcher).toString());

    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    System.out.println(hits[0]);
    System.out.println(hits[1]) ;

    writer.close();
    reader.close();
    store.close();
  }
}
