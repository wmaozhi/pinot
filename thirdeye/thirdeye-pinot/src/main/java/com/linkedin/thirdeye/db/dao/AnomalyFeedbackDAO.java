package com.linkedin.thirdeye.db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.jdbcx.JdbcDataSource;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.linkedin.thirdeye.dbi.GenericResultSetMapper;
import com.linkedin.thirdeye.dbi.SqlQueryBuilder;

public class AnomalyFeedbackDAO extends AbstractBaseDAO<AnomalyFeedback> {

  public AnomalyFeedbackDAO() {
    super(AnomalyFeedback.class);
  }

  public static void main(String[] args) throws Exception {
    JdbcDataSource ds = new JdbcDataSource();
    ds.setURL("jdbc:h2:mem:test");
    ds.setUser("sa");
    ds.setPassword("sa");
    Connection conn = ds.getConnection();
    //CREATE TABLE
    conn.createStatement().execute(
        "Create table anomaly_feedback (id bigint NOT NULL AUTO_INCREMENT, feedback_type varchar(150), comment varchar(500), status varchar(200))");
    ResultSet resultSet = conn.getMetaData().getTables(null, null, null, null);
    while (resultSet.next()) {
      System.out.println(resultSet.getString(3));
    }
    //create sql builder and register table in builder
    SqlQueryBuilder builder = new SqlQueryBuilder();
    builder.register(conn, AnomalyFeedback.class, "ANOMALY_FEEDBACK");

    //INSERT 3 rows
    AnomalyFeedbackDAO dao = new AnomalyFeedbackDAO();
    dao.connection = conn;
    dao.sqlQueryBuilder = builder;
    dao.genericResultSetMapper = new GenericResultSetMapper();
    for (int i = 0; i < 3; i++) {
      AnomalyFeedback feedback = new AnomalyFeedback();
      feedback.setComment("asdsad-" + i);
      feedback.setStatus(FeedbackStatus.NEW);
      feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
      Long feedbackId = dao.save(feedback);
      System.out.println("Saved Feedback ID:" + feedbackId);
    }
    //READ ALL ROWS
    ResultSet selectionResultSet =
        conn.createStatement().executeQuery("select * from anomaly_feedback");
    int count = 0;
    while (selectionResultSet.next()) {
      count++;
      System.out.println(selectionResultSet.getString(2));
    }
    System.out.println("Results found:" + count);
    //FIND BY ID
    AnomalyFeedback anomalyFeedback = dao.findById(1L);
    System.out.println("Retreived " + anomalyFeedback);

    //FIND BY PARAMS
    Map<String, Object> filters = new HashMap<>();
    filters.put("status", "NEW");
    List<AnomalyFeedback> results = dao.findByParams(filters);
    for (AnomalyFeedback result : results) {
      System.out.println("Retreived result: " + result);
    }

    //UPDATE TEST
    AnomalyFeedback updateFeedback = new AnomalyFeedback();
    updateFeedback.setId(1L);
    updateFeedback.setStatus(FeedbackStatus.RESOLVED);
    updateFeedback.setFeedbackType(AnomalyFeedbackType.NOT_ANOMALY);
    int updatedRows = dao.update(updateFeedback);
    System.out.println("Num rows Updated " + updatedRows);
    //READ THE UPDATED ROW
    AnomalyFeedback updatedFeedback = dao.findById(1L);
    System.out.println("Retreived updatedFeedback: " + updatedFeedback);

    //DELETE TEST
    int numRowsDeleted = dao.deleteById(1L);
    System.out.println("Num rows Deleted " + numRowsDeleted);
    //READ THE UPDATED ROW
    AnomalyFeedback deletedFeedback = dao.findById(1L);
    System.out.println("Retreived deletedFeedback must be null: " + deletedFeedback);
    

  }
}
