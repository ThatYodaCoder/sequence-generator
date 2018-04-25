package com.sequence.task;

import com.sequence.Sequence;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is intended for testing purpose
 */
public class SeqTestTask implements Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SeqTestTask.class);

  private final int taskId;
  private final Sequence sequence;
  private final Set numSet;
  private final long sleeptime;

  public SeqTestTask(Sequence sequence, Set numSet, int taskId, long sleepTime) {
    this.sequence = sequence;
    this.numSet = numSet;
    this.taskId = taskId;
    this.sleeptime = sleepTime;
  }

  @Override
  public Boolean call() throws Exception {
    Thread.sleep(sleeptime);

    for (int ctr = 0; ctr < 100000; ctr++) {
      long next = sequence.next();
      if (!numSet.contains(next) || next == -1) {
        numSet.add(next);
      } else {
        throw new Exception("Duplicate entry found. Key=" + next);
      }
      LOGGER.trace(" {} , seq_num={}", Thread.currentThread().getName(), next);
    }
    return Boolean.TRUE;
  }
}
