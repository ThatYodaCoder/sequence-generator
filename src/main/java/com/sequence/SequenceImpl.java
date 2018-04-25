package com.sequence;

import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SequenceImpl extends AbstractSequence {

  private static final Logger LOGGER = LoggerFactory.getLogger(SequenceImpl.class);

  private volatile SeqHolder seqHolder;


  public SequenceImpl(String sequenceName, DataSource dataSource) {
    super(sequenceName, dataSource);
    this.init();
    populateSequence(null);
  }

  @Override
  public long next() {

    SeqHolder tmpSeqHolder = seqHolder;
    int idx = tmpSeqHolder.getArrIdx().getAndIncrement();

    if (idx < tmpSeqHolder.getSequenceArrLength()) {

      long seqNum = tmpSeqHolder.getSequenceArr(idx);
      tmpSeqHolder.setElement(idx, -1);
      return seqNum;

    } else {
      this.populateSequence(tmpSeqHolder);
      return this.next();
    }
  }

  private synchronized void populateSequence(SeqHolder prevSeqHolder) {

    // Checking if the seqHolder is reinitialized by some other thread. If the prevSeqHolder is
    // different than the current Seq holder,
    // it means that seqHolder has been reinitialized by some other thread. In this case do not
    // reinitialize the sequence.
    // If previous seqHolder is same as current seqHolder, it means that this is the first
    // thread attempting to reinitialize the sequence after sequence is exhausted.
    if (prevSeqHolder == this.seqHolder) {

      long nextNum = nextNumGenerator.getNextNum(sequenceName);

      long[] sequenceArr = reInitializeSeqArr(nextNum);
      AtomicInteger arrIdx = new AtomicInteger(0);

      this.seqHolder = new SeqHolder(sequenceArr, arrIdx);

      noOfTimesPopulateInvoked++;

      LOGGER.trace("Sequence {} initialized, init count={}", this.sequenceName, noOfTimesPopulateInvoked);
    }
  }

  private long[] reInitializeSeqArr(long currSeqNum) {

    long[] seqArr = new long[incrementBy];
    long startSeqNum = currSeqNum;
    long upperBound = currSeqNum + (incrementBy - 1);
    int ctr = 0;
    while (startSeqNum <= upperBound) {
      seqArr[ctr] = startSeqNum;
      startSeqNum++;
      ctr++;
    }
    return seqArr;
  }

  public int getNoOfTimesPopulateInvoked() {
    return noOfTimesPopulateInvoked;
  }

  private static final class SeqHolder {

    private final long[] sequenceArr;
    private final AtomicInteger arrIdx;

    private SeqHolder(long[] sequenceArr, AtomicInteger arrIdx) {
      this.sequenceArr = sequenceArr.clone();
      this.arrIdx = arrIdx;
    }

    private long getSequenceArr(int idx) {
      return sequenceArr[idx];
    }

    private int getSequenceArrLength() {
      return sequenceArr.length;
    }

    private void setElement(int idx, long value) {
      sequenceArr[idx] = value;
    }

    private AtomicInteger getArrIdx() {
      return arrIdx;
    }
  }

}
