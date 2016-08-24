package com.linkedin.thirdeye.dbi.entity;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;

public class MergeConfig extends AbstractBaseEntity {
  private String mergeStrategy = AnomalyMergeStrategy.FUNCTION.name();
  private long sequentialAllowedGap = 30_000; // 30 seconds
  private long mergeDuration = 12 * 60 * 60 * 1000; // 12 hours
  private Long functionId;

  public MergeConfig() {

  }

  public MergeConfig(String mergeStrategy, long sequentialAllowedGap, long mergeDuration, Long functionId) {
    this.mergeDuration = mergeDuration;
    this.mergeStrategy = mergeStrategy;
    this.sequentialAllowedGap = sequentialAllowedGap;
    this.functionId = functionId;
  }

  public MergeConfig(Long id, String mergeStrategy, long sequentialAllowedGap, long mergeDuration, Long functionId) {
    setId(id);
    this.mergeDuration = mergeDuration;
    this.mergeStrategy = mergeStrategy;
    this.sequentialAllowedGap = sequentialAllowedGap;
    this.functionId = functionId;
  }

  public String getMergeStrategy() {
    return mergeStrategy;
  }

  public void setMergeStrategy(String mergeStrategy) {
    this.mergeStrategy = mergeStrategy;
  }

  public long getSequentialAllowedGap() {
    return sequentialAllowedGap;
  }

  public void setSequentialAllowedGap(long sequentialAllowedGap) {
    this.sequentialAllowedGap = sequentialAllowedGap;
  }

  public long getMergeDuration() {
    return mergeDuration;
  }

  public void setMergeDuration(long mergeDuration) {
    this.mergeDuration = mergeDuration;
  }

  public Long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(Long functionId) {
    this.functionId = functionId;
  }
}
