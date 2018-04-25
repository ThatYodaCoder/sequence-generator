package com.sequence.service;

import com.sequence.Sequence;
import com.sequence.exception.SequenceGenerationException;
import com.sequence.factory.SequenceFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OracleSequenceGeneratorService implements SequenceGeneratorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleSequenceGeneratorService.class);

  private List<String> seqIds;


  private DataSource dataSource;

  private Map<String, Sequence> sequenceMap;

  public OracleSequenceGeneratorService(DataSource dataSource) {
    seqIds = new ArrayList<>();
    this.dataSource = dataSource;
    init();
  }

  @Override
  public long geNextLongNum(String sequenceName) {
    return getSequenceMap().get(sequenceName).next();
  }

  protected List<String> getSeqIds() {

    String[] idArr = {};
    Properties appProps = new Properties();
    try {
      appProps.load(new FileInputStream("./src/main/resources/app.properties"));
      idArr = appProps.getProperty("seq-ids").split(",");
    } catch (IOException e) {
      throw new SequenceGenerationException("Error while reading properties file.", e);
    }
    return Arrays.asList(idArr);
  }

  public void setSeqIds(List<String> seqIds) {
    this.seqIds = seqIds;
  }

  protected void init() {
    Map<String, Sequence> map = new HashMap<>();
    for (String sequenceName : getSeqIds()) {
      LOGGER.info("Initializing sequence {}", sequenceName);
      Sequence sequence = SequenceFactory.getSequence(sequenceName, dataSource);
      map.put(sequenceName, sequence);
    }
    sequenceMap = Collections.unmodifiableMap(map);
  }

  protected Map<String, Sequence> getSequenceMap() {
    return sequenceMap;
  }

}
