package com.sequence;

import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SequenceImplOpt extends AbstractSequence {

  private static final Logger LOGGER = LoggerFactory.getLogger(SequenceImplOpt.class);

  private volatile SeqHolder seqHolder;

  public SequenceImplOpt(String sequenceName, DataSource dataSource) {
    super(sequenceName, dataSource);
    this.init();
    populateSequence(null);
  }


  @Override
  public long next() {

    SeqHolder tmpSeqHolder = seqHolder;
    long seqNum = tmpSeqHolder.getCurrentValue().getAndIncrement();

    if (seqNum <= tmpSeqHolder.getUpperBound()) {
      return seqNum;
    } else {
      this.populateSequence(tmpSeqHolder);
      return this.next();
    }
  }

  private synchronized void populateSequence(SeqHolder prevSeqHolder) {
    /**
     * Checking if the seqHolder is reinitialized by some other thread. If the prevSeqHolder is different than the current Seq holder,
     * it means that seqHolder has been reinitialized by some other thread. In this case do not reinitialize the sequence.
     * If previous seqHolder is same as current seqHolder, it means that this is the first thread attempting to reinitialize the sequence after sequence is exhausted.
     */

    if (prevSeqHolder == this.seqHolder) {
      long nextNum = nextNumGenerator.getNextNum(sequenceName);
      this.seqHolder = getReInitializedSeq(nextNum);
      noOfTimesPopulateInvoked++;
      LOGGER.trace("Sequence {} initialized, init count={}", this.sequenceName, noOfTimesPopulateInvoked);
    }
  }


  private SeqHolder getReInitializedSeq(long currSeqNum) {
    long upperBound = currSeqNum + (incrementBy - 1);
    return new SeqHolder(new AtomicLong(currSeqNum), upperBound);
  }

  public int getNoOfTimesPopulateInvoked() {
    return noOfTimesPopulateInvoked;
  }

  private static final class SeqHolder {

    private final AtomicLong currentValue;
    private final long upperBound;

    private SeqHolder(AtomicLong currentValue, long upperBound) {
      this.currentValue = currentValue;
      this.upperBound = upperBound;
    }

    public AtomicLong getCurrentValue() {
      return currentValue;
    }

    public long getUpperBound() {
      return upperBound;
    }
  }


}
