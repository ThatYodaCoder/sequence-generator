package com.sequence.generator;

import com.sequence.exception.SequenceGenerationException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleNextNumGenerator implements NextNumGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleNextNumGenerator.class);

  private static final String NEXT_SEQ_SQL = "SELECT {0}.NEXTVAL FROM dual";
  private static final String SEQ_INFO_SQL = "select INCREMENT_BY from USER_SEQUENCES where SEQUENCE_NAME = ?";
  private final DataSource dataSource;

  public OracleNextNumGenerator(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public long getNextNum(String seqName) {

    LOGGER.trace("Getting next val for sequence {}", seqName);

    String sql = MessageFormat.format(NEXT_SEQ_SQL, seqName);

    try (Connection connection = dataSource.getConnection();
        PreparedStatement pStmt = connection.prepareStatement(sql);
        ResultSet resultSet = pStmt.executeQuery()) {

      int nextValue = 0;
      while (resultSet.next()) {
        nextValue = resultSet.getInt(1);
      }
      return nextValue;

    } catch (SQLException e) {
      throw new SequenceGenerationException(e.getMessage(), e);
    }
  }

  @Override
  public int getIncrementByValue(String seqName) {
    int incrementByValue = 0;
    LOGGER.trace("Getting increment by value for sequence {}", seqName);

    try (Connection connection = dataSource.getConnection();
        PreparedStatement pStmt = connection.prepareStatement(SEQ_INFO_SQL)) {

      pStmt.setString(1, seqName);

      try (ResultSet resultSet = pStmt.executeQuery()) {
        while (resultSet.next()) {
          incrementByValue = resultSet.getInt(1);
        }
      }
    } catch (SQLException e) {
      throw new SequenceGenerationException(e.getMessage(), e);
    }
    return incrementByValue;
  }
}
