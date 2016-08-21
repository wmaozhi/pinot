package com.linkedin.thirdeye.db.dao;

import java.sql.Connection;
import java.sql.ResultSet;

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
    conn.createStatement().execute(
        "Create table anomaly_feedback (id bigint NOT NULL AUTO_INCREMENT, feedback_type varchar(150), comment varchar(500), status varchar(200))");
    ResultSet resultSet = conn.getMetaData().getTables(null, null, null, null);
    while (resultSet.next()) {
      System.out.println(resultSet.getString(3));
    }
    SqlQueryBuilder builder = new SqlQueryBuilder();
    builder.register(conn, AnomalyFeedback.class, "ANOMALY_FEEDBACK");

    AnomalyFeedbackDAO dao = new AnomalyFeedbackDAO();
    dao.connection = conn;
    dao.sqlQueryBuilder = builder;
    dao.genericResultSetMapper = new GenericResultSetMapper();
    AnomalyFeedback feedback = new AnomalyFeedback();
    feedback.setComment("asdsad");
    feedback.setStatus(FeedbackStatus.NEW);
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    Long feedbackId = dao.save(feedback);
    System.out.println("Feedback ID:" + feedbackId);
    ResultSet selectionResultSet =
        conn.createStatement().executeQuery("select * from anomaly_feedback");
    int count = 0;
    while (selectionResultSet.next()) {
      count++;
      System.out.println(selectionResultSet.getString(2));
    }
    System.out.println("Results found:" + count);
    AnomalyFeedback anomalyFeedback = dao.findById(1L);
    System.out.println("Retreived " + anomalyFeedback);
  }
}
