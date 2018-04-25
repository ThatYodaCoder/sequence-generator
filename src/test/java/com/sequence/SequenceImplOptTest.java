package com.sequence;

import com.sequence.exception.SequenceGenerationException;
import com.sequence.generator.NextNumGenerator;
import com.sequence.generator.factory.NextNumGeneratorFactory;
import com.sequence.task.SeqTestTask;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class SequenceImplOptTest {

  private final int incrementByValue = 50;

  @Test
  public void testNext() throws SequenceGenerationException, ExecutionException, InterruptedException {
    for (int noOfThreads = 1; noOfThreads <= 40; noOfThreads++) {

      SortedSet<Long> treeSet = Collections.synchronizedSortedSet(new TreeSet<>());

      System.out.println("No of Threads=" + noOfThreads);

      SequenceImplOpt seqImpl = getSeqImpl();

      ExecutorService executorService = Executors.newFixedThreadPool(noOfThreads);

      List<Future<Boolean>> futureList = new ArrayList<>();

      long sleepTime = 600;
      for (int ctr = 0; ctr < 40; ctr++) {
        futureList.add(executorService.submit(new SeqTestTask(seqImpl, treeSet, ctr, sleepTime)));
        sleepTime = sleepTime - 15;
      }

      for (Future<Boolean> future : futureList) {
        Assert.assertTrue(future.get().booleanValue());
      }

      System.out.println("No of Threads=" + noOfThreads);

      Assert.assertEquals(400000, seqImpl.getNoOfTimesPopulateInvoked());

      Assert.assertEquals(1, treeSet.first().longValue());
      Assert.assertEquals(4000000, treeSet.last().longValue());
      Assert.assertEquals(4000000, treeSet.size());
      executorService.shutdown();
    }
  }

  private void multithreadedTester() throws SequenceGenerationException, ExecutionException, InterruptedException {

    for (int noOfThreads = 1; noOfThreads <= 40; noOfThreads++) {

      SortedSet<Long> treeSet = Collections.synchronizedSortedSet(new TreeSet<>());

      System.out.println("No of Threads=" + noOfThreads);

      SequenceImplOpt seqImpl = getSeqImpl();

      ExecutorService executorService = Executors.newFixedThreadPool(noOfThreads);

      List<Future<Boolean>> futureList = new ArrayList<>();

      long sleepTime = 600;
      for (int ctr = 0; ctr < 40; ctr++) {
        futureList.add(executorService.submit(new SeqTestTask(seqImpl, treeSet, ctr, sleepTime)));
        sleepTime = sleepTime - 15;
      }

      for (Future<Boolean> future : futureList) {
        Assert.assertTrue(future.get().booleanValue());
      }

      System.out.println("No of Threads=" + noOfThreads);

      Assert.assertEquals(400000, seqImpl.getNoOfTimesPopulateInvoked());

      Assert.assertEquals(1, treeSet.first().longValue());
      Assert.assertEquals(4000000, treeSet.last().longValue());
      Assert.assertEquals(4000000, treeSet.size());
      executorService.shutdown();
    }
  }

  private SequenceImplOpt getSeqImpl() throws SequenceGenerationException {

    new MockUp<NextNumGeneratorFactory>() {

      @Mock
      public NextNumGenerator getNextNumGenerator(DataSource dataSource) {
        {
          return new NextNumGenerator() {

            private int seqValue = 1;
            private int incrementBy = 10;
            private boolean begin;

            @Override
            public long getNextNum(String seqName) {
              if (!begin) {
                begin = true;
                return seqValue;
              } else {
                seqValue = seqValue + 10;
                return seqValue;
              }
            }

            @Override
            public int getIncrementByValue(String seqName) {
              return incrementBy;
            }
          };
        }
      }
    };

    return new SequenceImplOpt("TestSeq", null);
  }
}
