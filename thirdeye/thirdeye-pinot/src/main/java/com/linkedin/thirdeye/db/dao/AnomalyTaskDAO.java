package com.linkedin.thirdeye.db.dao;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;

public class AnomalyTaskDAO extends AbstractBaseDAO<AnomalyTaskSpec> {

  private static final String FIND_BY_JOB_ID_STATUS_NOT_IN = "SELECT * FROM AnomalyTaskSpec"
      + "WHERE jobId = :jobId " + "AND status != :status";

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC =
      "SELECT * FROM AnomalyTaskSpec " + "WHERE status = :status "
          + "order by taskStartTime asc";

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE =
      "SELECT FROM AnomalyTaskSpec "
          + "WHERE status = :status AND lastModified < :expireTimestamp";

  public AnomalyTaskDAO() {
    super(AnomalyTaskSpec.class);
  }

  public List<AnomalyTaskSpec> findByJobIdStatusNotIn(Long jobId, TaskStatus status) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("jobId", jobId);
    parameterMap.put("status", status);
    return executeParameterizedSQL(FIND_BY_JOB_ID_STATUS_NOT_IN, parameterMap);
  }

  public List<AnomalyTaskSpec> findByStatusOrderByCreateTimeAscending(TaskStatus status) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("status", status);
    return executeParameterizedSQL(FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC, parameterMap);
  }

  public List<AnomalyTaskSpec> findByIdAndStatus(Long id, TaskStatus status) {
    return super.findByParams(ImmutableMap.of("status", status, "id", id));
  }

  public boolean updateStatusAndWorkerId(Long workerId, Long id, TaskStatus oldStatus,
      TaskStatus newStatus) {
    AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
    anomalyTaskSpec.setId(id);
    anomalyTaskSpec.setStatus(newStatus);
    anomalyTaskSpec.setWorkerId(workerId);
    Set<String> fieldsToUpdate = Sets.newHashSet("status", "workerId");
    return (update(anomalyTaskSpec, fieldsToUpdate) == 1);
  }

  public boolean updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime) {
    AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
    anomalyTaskSpec.setId(id);
    anomalyTaskSpec.setStatus(newStatus);
    anomalyTaskSpec.setTaskEndTime(taskEndTime);
    Set<String> fieldsToUpdate = Sets.newHashSet("status", "taskEndTime");
    return (update(anomalyTaskSpec, fieldsToUpdate) == 1);
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("expireTimestamp", expireTimestamp);
    parameterMap.put("status", status);
    List<AnomalyTaskSpec> anomalyTaskSpecs = executeParameterizedSQL(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, parameterMap);
    for (AnomalyTaskSpec anomalyTaskSpec : anomalyTaskSpecs) {
      deleteById(anomalyTaskSpec.getId());
    }
    return anomalyTaskSpecs.size();
  }
}
