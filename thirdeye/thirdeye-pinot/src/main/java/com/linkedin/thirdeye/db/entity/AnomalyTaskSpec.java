package com.linkedin.thirdeye.db.entity;

import java.sql.Timestamp;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.JoinColumn;
import javax.persistence.Table;
import javax.persistence.Version;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly
 * job, which in turn spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the
 * workers
 */
@Entity
@Table(name = "anomaly_tasks")
public class AnomalyTaskSpec extends AbstractBaseEntity {

  @JoinColumn(name = "job_id")
  private Long jobId;

  @Enumerated(EnumType.STRING)
  @Column(name = "task_type", nullable = false)
  private TaskType taskType;

  @Column(name = "worker_id")
  private Long workerId;

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private TaskStatus status;

  @Column(name = "task_start_time")
  private long taskStartTime;

  @Column(name = "task_end_time")
  private long taskEndTime;

  @Column(name = "task_info", nullable = false)
  private String taskInfo;

  @Column(name = "last_modified", insertable = false, updatable = false,
      columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
  private Timestamp lastModified;

  @Version
  @Column(name = "version", columnDefinition = "integer DEFAULT 0", nullable = false)
  private int version = 0;


  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  public Long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobName() {
    return jobName;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public void setStatus(TaskStatus status) {
    this.status = status;
  }

  public long getTaskStartTime() {
    return taskStartTime;
  }

  public void setTaskStartTime(long startTime) {
    this.taskStartTime = startTime;
  }

  public long getTaskEndTime() {
    return taskEndTime;
  }

  public void setTaskEndTime(long endTime) {
    this.taskEndTime = endTime;
  }

  public String getTaskInfo() {
    return taskInfo;
  }

  public void setTaskInfo(String taskInfo) {
    this.taskInfo = taskInfo;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }


  public Timestamp getLastModified() {
    return lastModified;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyTaskSpec)) {
      return false;
    }
    AnomalyTaskSpec af = (AnomalyTaskSpec) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(status, af.getStatus())
        && Objects.equals(taskStartTime, af.getTaskStartTime())
        && Objects.equals(taskEndTime, af.getTaskEndTime())
        && Objects.equals(taskInfo, af.getTaskInfo()) && Objects.equals(jobId, af.getJobId())
        && Objects.equals(workerId, af.getWorkerId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), jobId, status, taskStartTime, taskEndTime, taskInfo, workerId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("jobId", getJobId())
        .add("status", status).add("startTime", taskStartTime).add("endTime", taskEndTime)
        .add("taskInfo", taskInfo).add("lastModified", lastModified).add("workerId", workerId)
        .toString();
  }
}
