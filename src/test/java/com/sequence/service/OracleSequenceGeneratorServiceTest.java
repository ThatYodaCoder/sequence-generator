package com.sequence.service;

import com.sequence.Sequence;
import com.sequence.factory.SequenceFactory;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import mockit.Expectations;
import mockit.Injectable;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import org.junit.Assert;
import org.junit.Test;

public class OracleSequenceGeneratorServiceTest {

  @Tested
  public OracleSequenceGeneratorService seqGenService;

  @Injectable
  public DataSource dataSource;

  private List<String> sequenceIds = new ArrayList<String>((Arrays
      .asList("ID_TEST_1", "ID_TEST_2", "ID_TEST_3", "ID_TEST_4", "ID_TEST_5", "ID_TEST_6", "ID_TEST_7", "ID_TEST_8")));

  @Mocked
  private Sequence mockedSequence;

  @Test
  public void testGetNextLongNum() {

    Map<String, Sequence> sequenceMap = getSequenceMap();

    new MockUp<SequenceFactory>() {
      public Sequence getSequence(String sequenceName, Connection connection) {
        return sequenceMap.get(sequenceName);
      }
    };

    new Expectations(OracleSequenceGeneratorService.class) {
      {
        seqGenService.getSequenceMap();
        result = sequenceMap;
        times = 8;

      }
    };

    Assert.assertEquals("Getting next value for sequence ID_TEST_1", 101,
        seqGenService.geNextLongNum("ID_TEST_1"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_2", 201,
        seqGenService.geNextLongNum("ID_TEST_2"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_3", 301,
        seqGenService.geNextLongNum("ID_TEST_3"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_4", 401,
        seqGenService.geNextLongNum("ID_TEST_4"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_5", 501,
        seqGenService.geNextLongNum("ID_TEST_5"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_6", 601,
        seqGenService.geNextLongNum("ID_TEST_6"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_7", 701,
        seqGenService.geNextLongNum("ID_TEST_7"));
    Assert.assertEquals("Getting next value for sequence ID_TEST_8", 801,
        seqGenService.geNextLongNum("ID_TEST_8"));


  }

  private Map<String, Sequence> getSequenceMap() {

    Map<String, Sequence> sequenceMap = new HashMap<>();

    long startVal = 0;

    for (String sequenceId : sequenceIds) {

      startVal = startVal + 100;

      final long tempVal = startVal;

      Sequence sequence = new Sequence() {

        private long value = tempVal;

        @Override
        public long next() {
          return ++value;
        }
      };

      sequenceMap.put(sequenceId, sequence);
    }

    return sequenceMap;
  }

  @Test
  public void testInit() {

    new MockUp<SequenceFactory>() {
      public Sequence getSequence(String sequenceName, Connection connection) {
        return mockedSequence;
      }
    };

    new Expectations(OracleSequenceGeneratorService.class) {
      {
        seqGenService.getSeqIds();
        result = sequenceIds;
        times = 1;

      }
    };

    seqGenService.init();
    Assert.assertEquals("Number of Sequences Initialized", 8, seqGenService.getSequenceMap().size());

  }
}