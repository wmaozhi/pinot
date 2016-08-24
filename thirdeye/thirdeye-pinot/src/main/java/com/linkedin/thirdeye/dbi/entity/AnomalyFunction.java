package com.linkedin.thirdeye.dbi.entity;

import java.util.concurrent.TimeUnit;


public class AnomalyFunction extends AbstractEntity {
  private String collection;

  private String functionName;

  private String metric;

  private String metricFunction;

  private String type;

  private boolean active = true;

  private String properties;

  private String cron;

  private Integer bucketSize;

  private TimeUnit bucketUnit;

  private Integer windowSize;

  private TimeUnit windowUnit;

  private Integer windowDelay;

  private TimeUnit windowDelayUnit;

  private String exploreDimensions;

  private String filters;

  public AnomalyFunction () {

  }
  public AnomalyFunction(boolean active, Integer bucketSize, TimeUnit bucketUnit, String collection,
      String cron, String exploreDimensions, String filters, String functionName, String metric,
      String metricFunction, String properties, String type, Integer windowDelay,
      TimeUnit windowDelayUnit, Integer windowSize, TimeUnit windowUnit) {
    this(null, active, bucketSize, bucketUnit, collection, cron, exploreDimensions, filters, functionName, metric,
        metricFunction, properties, type, windowDelay, windowDelayUnit, windowSize, windowUnit);
  }

  public AnomalyFunction(Long id, boolean active, Integer bucketSize, TimeUnit bucketUnit, String collection,
      String cron, String exploreDimensions, String filters, String functionName, String metric,
      String metricFunction, String properties, String type, Integer windowDelay,
      TimeUnit windowDelayUnit, Integer windowSize, TimeUnit windowUnit) {
    super(id);
    this.active = active;
    this.bucketSize = bucketSize;
    this.bucketUnit = bucketUnit;
    this.collection = collection;
    this.cron = cron;
    this.exploreDimensions = exploreDimensions;
    this.filters = filters;
    this.functionName = functionName;
    this.metric = metric;
    this.metricFunction = metricFunction;
    this.properties = properties;
    this.type = type;
    this.windowDelay = windowDelay;
    this.windowDelayUnit = windowDelayUnit;
    this.windowSize = windowSize;
    this.windowUnit = windowUnit;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getMetricFunction() {
    return metricFunction;
  }

  public void setMetricFunction(String metricFunction) {
    this.metricFunction = metricFunction;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public Integer getBucketSize() {
    return bucketSize;
  }

  public void setBucketSize(Integer bucketSize) {
    this.bucketSize = bucketSize;
  }

  public TimeUnit getBucketUnit() {
    return bucketUnit;
  }

  public void setBucketUnit(TimeUnit bucketUnit) {
    this.bucketUnit = bucketUnit;
  }

  public Integer getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(Integer windowSize) {
    this.windowSize = windowSize;
  }

  public TimeUnit getWindowUnit() {
    return windowUnit;
  }

  public void setWindowUnit(TimeUnit windowUnit) {
    this.windowUnit = windowUnit;
  }

  public Integer getWindowDelay() {
    return windowDelay;
  }

  public void setWindowDelay(Integer windowDelay) {
    this.windowDelay = windowDelay;
  }

  public TimeUnit getWindowDelayUnit() {
    return windowDelayUnit;
  }

  public void setWindowDelayUnit(TimeUnit windowDelayUnit) {
    this.windowDelayUnit = windowDelayUnit;
  }

  public String getExploreDimensions() {
    return exploreDimensions;
  }

  public void setExploreDimensions(String exploreDimensions) {
    this.exploreDimensions = exploreDimensions;
  }

  public String getFilters() {
    return filters;
  }

  public void setFilters(String filters) {
    this.filters = filters;
  }
}
