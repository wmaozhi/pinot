package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class AnomalyFunctionDAO extends AbstractBaseDAO<AnomalyFunctionSpec> {

  public AnomalyFunctionDAO() {
    super(AnomalyFunctionSpec.class);
  }

  public List<AnomalyFunctionSpec> findAllByCollection(String collection) {
    return super.findByParams(ImmutableMap.of("collection", collection));
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    Set<String> uniqueMetricSet = new TreeSet<>();
    List<AnomalyFunctionSpec> findByParams =
        super.findByParams(ImmutableMap.of("collection", collection));
    for (AnomalyFunctionSpec anomalyFunctionSpec : findByParams) {
      uniqueMetricSet.add(anomalyFunctionSpec.getMetric());
    }
    return new ArrayList<>(uniqueMetricSet);
  }

  public List<AnomalyFunctionSpec> findAllActiveFunctions() {
    return super.findByParams(ImmutableMap.of("isActive", true));
  }
}
