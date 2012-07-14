package org.apache.lucene.search.poshighlight;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.nestedpulsing.NestedPulsingPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.positions.BlockPositionIterator;
import org.apache.lucene.search.positions.PositionFilterQuery;
import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import java.io.IOException;

/**
 * TODO: FIX THIS TEST Phrase and Span Queries positions callback API
 */
public class PosHighlighterTest extends LuceneTestCase {
  
  protected final static String F = "f";
  protected Analyzer analyzer;
  protected Directory dir;
  protected IndexSearcher searcher;
  private IndexWriterConfig iwc;
  
  private static final String PORRIDGE_VERSE = "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
      + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!";
  
  public void setUp() throws Exception {
    super.setUp();
    // Currently only SimpleText and Lucene40 can index offsets into postings:
    String codecName = Codec.getDefault().getName();
    assumeTrue("Codec does not support offsets: " + codecName,
        codecName.equals("SimpleText") || codecName.equals("Lucene40"));
    iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).setOpenMode(OpenMode.CREATE);

    if (codecName.equals("Lucene40")) {
      // Sep etc are not implemented
      switch(random().nextInt(4)) {
        case 0: iwc.setCodec(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat())); break;
        case 1: iwc.setCodec(_TestUtil.alwaysPostingsFormat(new MemoryPostingsFormat())); break;
        case 2: iwc.setCodec(_TestUtil.alwaysPostingsFormat(
            new Pulsing40PostingsFormat(_TestUtil.nextInt(random(), 1, 3)))); break;
        case 3: iwc.setCodec(_TestUtil.alwaysPostingsFormat(new NestedPulsingPostingsFormat())); break;
      }
    }
    analyzer = iwc.getAnalyzer();
    dir = newDirectory();
  }
  
  public void close() throws IOException {
    if (searcher != null) {
      searcher.getIndexReader().close();
      searcher = null;
    }
    dir.close();
  }
  
  // make several docs
  protected void insertDocs(Analyzer analyzer, String... values)
      throws Exception {
    IndexWriter writer = new IndexWriter(dir, iwc);
    FieldType type = new FieldType();
    type.setIndexed(true);
    type.setTokenized(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    type.setStored(true);
    for (String value : values) {
      Document doc = new Document();
      Field f = newField(F, value, type);
      doc.add(f);
      writer.addDocument(doc);
    }
    writer.close();
    if (searcher != null) {
      searcher.getIndexReader().close();
    }
    searcher = new IndexSearcher(DirectoryReader.open(dir));
  }
  
  private String[] doSearch(Query q) throws IOException,
      InvalidTokenOffsetsException {
    return doSearch(q, 100);
  }
  
  private class ConstantScorer implements
      org.apache.lucene.search.highlight.Scorer {
    
    @Override
    public TokenStream init(TokenStream tokenStream) throws IOException {
      return tokenStream;
    }
    
    @Override
    public void startFragment(TextFragment newFragment) {}
    
    @Override
    public float getTokenScore() {
      return 1;
    }
    
    @Override
    public float getFragmentScore() {
      return 1;
    }
  }
  
  private String[] doSearch(Query q, int maxFragSize) throws IOException,
      InvalidTokenOffsetsException {
    return doSearch(q, maxFragSize, 0);
  }
  
  private String[] doSearch(Query q, int maxFragSize, int docIndex)
      throws IOException, InvalidTokenOffsetsException {
    // ConstantScorer is a fragment Scorer, not a search result (document)
    // Scorer
    Highlighter highlighter = new Highlighter(new ConstantScorer());
    highlighter.setTextFragmenter(new SimpleFragmenter(maxFragSize));
    PosCollector collector = new PosCollector(10);
    if (q instanceof MultiTermQuery) {
      ((MultiTermQuery) q)
          .setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    }
    searcher.search(q, collector);
    ScorePosDoc doc = collector.docs[docIndex];
    if (doc == null) return null;
    String text = searcher.getIndexReader().document(doc.doc).get(F);
    // FIXME: test error cases: for non-stored fields, and fields w/no term
    // vectors
    // searcher.getIndexReader().getTermFreqVector(doc.doc, F, pom);
    
    TextFragment[] fragTexts = highlighter.getBestTextFragments(
        new PosTokenStream(text, new PositionIntervalArrayIterator(doc
            .sortedPositions(), doc.posCount)), text, false, 10);
    String[] frags = new String[fragTexts.length];
    for (int i = 0; i < frags.length; i++)
      frags[i] = fragTexts[i].toString();
    return frags;
  }
  
  public void testTerm() throws Exception {
    insertDocs(analyzer, "This is a test test");
    String frags[] = doSearch(new TermQuery(new Term(F, "test")));
    assertEquals("This is a <B>test</B> <B>test</B>", frags[0]);
    close();
  }
  
  public void testSeveralSnippets() throws Exception {
    String input = "this is some long text.  It has the word long in many places.  In fact, it has long on some different fragments.  "
        + "Let us see what happens to long in this case.";
    String gold = "this is some <B>long</B> text.  It has the word <B>long</B> in many places.  In fact, it has <B>long</B> on some different fragments.  "
        + "Let us see what happens to <B>long</B> in this case.";
    insertDocs(analyzer, input);
    String frags[] = doSearch(new TermQuery(new Term(F, "long")),
        input.length());
    assertEquals(gold, frags[0]);
    close();
  }
  
  public void testBooleanAnd() throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.MUST));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testBooleanAndOtherOrder() throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.MUST));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testBooleanOr() throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.SHOULD));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testSingleMatchScorer() throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "notoccurringterm")),
        Occur.SHOULD));
    String frags[] = doSearch(bq);
    assertEquals("This is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testBooleanNrShouldMatch() throws Exception {
    insertDocs(analyzer, "a b c d e f g h i");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "a")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "b")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "no")), Occur.SHOULD));
    
    // This generates a ConjunctionSumScorer
    bq.setMinimumNumberShouldMatch(2);
    String frags[] = doSearch(bq);
    assertEquals("<B>a</B> <B>b</B> c d e f g h i", frags[0]);
    
    // This generates no scorer
    bq.setMinimumNumberShouldMatch(3);
    frags = doSearch(bq);
    assertNull(frags);
    
    // This generates a DisjunctionSumScorer
    bq.setMinimumNumberShouldMatch(2);
    bq.add(new BooleanClause(new TermQuery(new Term(F, "c")), Occur.SHOULD));
    frags = doSearch(bq);
    assertEquals("<B>a</B> <B>b</B> <B>c</B> d e f g h i", frags[0]);
    close();
  }
  
  public void testPhrase() throws Exception {
    insertDocs(analyzer, "is it that this is a test, is it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "is")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "a")), Occur.MUST));
    PositionFilterQuery pfq = new PositionFilterQuery(bq,
        new BlockPositionIteratorFilter());
    String frags[] = doSearch(pfq);
    // make sure we highlight the phrase, and not the terms outside the phrase
    assertEquals("is it that this <B>is</B> <B>a</B> test, is it", frags[0]);
    close();
  }
  
  /*
   * Failing ... PhraseQuery scorer needs positions()?
   */
  //@Ignore
  public void testPhraseOriginal() throws Exception {
    insertDocs(analyzer, "This is a test");
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(F, "a"));
    pq.add(new Term(F, "test"));
    String frags[] = doSearch(pq);
    assertEquals("This is <B>a</B> <B>test</B>", frags[0]);
    close();
  }
  
  public void testNestedBoolean() throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.SHOULD));
    bq2.add(new BooleanClause(new TermQuery(new Term(F, "is")), Occur.SHOULD));
    bq.add(new BooleanClause(bq2, Occur.SHOULD));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> <B>is</B> a <B>test</B>", frags[0]);
    close();
  }
  
  public void testWildcard() throws Exception {
    insertDocs(analyzer, "This is a test");
    String frags[] = doSearch(new WildcardQuery(new Term(F, "t*t")));
    assertEquals("This is a <B>test</B>", frags[0]);
    close();
  }

  public void testMixedBooleanNot() throws Exception {
    insertDocs(analyzer, "this is a test", "that is an elephant");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "that")), Occur.MUST_NOT));
    String frags[] = doSearch(bq);
    assertEquals("this is a <B>test</B>", frags[0]);
    close();
  }

  public void testMixedBooleanShould() throws Exception {
    insertDocs(analyzer, "this is a test", "that is an elephant", "the other was a rhinoceros");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "is")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    String frags[] = doSearch(bq, 50, 0);
    assertEquals("this <B>is</B> a <B>test</B>", frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("that <B>is</B> an elephant", frags[0]);

    bq.add(new BooleanClause(new TermQuery(new Term(F, "rhinoceros")), Occur.SHOULD));
    frags = doSearch(bq, 50, 0);
    assertEquals("this <B>is</B> a <B>test</B>", frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("that <B>is</B> an elephant", frags[0]);
    close();
  }
  
  public void testMultipleDocumentsAnd() throws Exception {
    insertDocs(analyzer, "This document has no matches", PORRIDGE_VERSE,
        "This document has some Pease porridge in it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "Pease")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "porridge")), Occur.MUST));
    String frags[] = doSearch(bq, 50, 0);
    assertEquals(
        "<B>Pease</B> <B>porridge</B> hot! <B>Pease</B> <B>porridge</B> cold! <B>Pease</B>",
        frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("This document has some <B>Pease</B> <B>porridge</B> in it",
        frags[0]);
    close();
  }
  

  public void testMultipleDocumentsOr() throws Exception {
    insertDocs(analyzer, "This document has no matches", PORRIDGE_VERSE,
        "This document has some Pease porridge in it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "Pease")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "porridge")),
        Occur.SHOULD));
    String frags[] = doSearch(bq, 50, 0);
    assertEquals(
        "<B>Pease</B> <B>porridge</B> hot! <B>Pease</B> <B>porridge</B> cold! <B>Pease</B>",
        frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("This document has some <B>Pease</B> <B>porridge</B> in it",
        frags[0]);
    close();
  }
  
  public static class BlockPositionIteratorFilter implements PositionIntervalFilter {

    @Override
    public PositionIntervalIterator filter(PositionIntervalIterator iter) {
      return new BlockPositionIterator(true, iter);
    }
    
  }
  
}