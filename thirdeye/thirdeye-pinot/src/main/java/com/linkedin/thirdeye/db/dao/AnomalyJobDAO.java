package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

public class AnomalyJobDAO extends AbstractBaseDAO<AnomalyJobSpec> {

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE =
      "select * from AnomalyJobSpec  "
          + "WHERE status = :status AND lastModified < :lastModified";

  public AnomalyJobDAO() {
    super(AnomalyJobSpec.class);
  }

  public List<AnomalyJobSpec> findByStatus(JobStatus status) {
    return super.findByParams(ImmutableMap.of("status", status));
  }

  public void updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime) {
    AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
    anomalyJobSpec.setId(id);
    anomalyJobSpec.setStatus(status);
    anomalyJobSpec.setScheduleEndTime(jobEndTime);
    Set<String> fieldsToUpdate = Sets.newHashSet("status", "scheduleEndTime");
    update(anomalyJobSpec, fieldsToUpdate);
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("lastModified", expireTimestamp);
    parameterMap.put("status", status);
    List<AnomalyJobSpec> anomalyJobSpecs =
        executeParameterizedSQL(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, parameterMap);
    if (anomalyJobSpecs != null) {
      for (AnomalyJobSpec anomalyJobSpec : anomalyJobSpecs) {
        deleteById(anomalyJobSpec.getId());
      }
      return anomalyJobSpecs.size();
    }
    return 0;
  }
}
