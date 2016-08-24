package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnomalyMergedResultDAO extends AbstractBaseDAO<AnomalyMergedResult> {

  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME =
      "from AnomalyMergedResult amr where amr.collection=:collection and amr.metric=:metric "
          + "and amr.dimensions=:dimensions order by amr.endTime desc limit 1";

  private static final String FIND_BY_FUNCTION_AND_DIMENSIONS =
      "from AnomalyMergedResult amr where amr.function.id=:functionId "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  private static final String FIND_BY_FUNCTION_AND_NULL_DIMENSION =
      "from AnomalyMergedResult amr where amr.function.id=:functionId "
          + "and amr.dimensions is null order by amr.endTime desc";

  private static final String FIND_BY_TIME =
      "SELECT r FROM AnomalyMergedResult r WHERE ((r.startTime >= :startTime AND r.startTime <= :endTime) "
          + "OR (r.endTime >= :startTime AND r.endTime <= :endTime)) order by r.endTime desc ";

  public AnomalyMergedResultDAO() {
    super(AnomalyMergedResult.class);
  }

  public List<AnomalyMergedResult> getAllByTime(long startTime, long endTime) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("startTime", startTime);
    parameterMap.put("endTime", endTime);
    return executeParameterizedSQL(FIND_BY_TIME, parameterMap);
  }

  public List<AnomalyMergedResult> findByCollectionMetricDimensions(String collection,
      String metric, String dimensions) {
    Map<String, Object> params = new HashMap<>();
    params.put("collection", collection);
    params.put("metric", metric);
    params.put("dimensions", dimensions);
    return super.findByParams(params);
  }

  public AnomalyMergedResult findLatestByCollectionMetricDimensions(String collection,
      String metric, String dimensions) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("collection", collection);
    parameterMap.put("metric", metric);
    parameterMap.put("dimensions", dimensions);
    List<AnomalyMergedResult> results = executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME, parameterMap);
    if (results.size() > 0) {
      return results.get(0);
    }
    return null;
  }

  public AnomalyMergedResult findLatestByFunctionIdDimensions(Long functionId, String dimensions) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionId", functionId);
    parameterMap.put("dimensions", dimensions);
    List<AnomalyMergedResult> results =
        executeParameterizedSQL(FIND_BY_FUNCTION_AND_DIMENSIONS, parameterMap);
    if (results.size() > 0) {
      return results.get(0);
    }
    return null;
  }

  public AnomalyMergedResult findLatestByFunctionIdOnly(Long functionId) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionId", functionId);
    List<AnomalyMergedResult> results =
        executeParameterizedSQL(FIND_BY_FUNCTION_AND_NULL_DIMENSION, parameterMap);
    if (results.size() > 0) {
      return results.get(0);
    }
    return null;

  }
}
