package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class AnomalyResultDAO extends AbstractBaseDAO<AnomalyResult> {

  private static final String FIND_BY_COLLECTION_TIME =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection AND r.function.metric = :metric "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC_DIMENSION =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection AND r.function.metric = :metric "
          + "AND r.dimensions IN :dimensions "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc)) ";

  private static final String FIND_BY_COLLECTION_TIME_FILTERS =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection "
          + "AND ((r.function.filters = :filters) or (r.function.filters is NULL and :filters is NULL)) "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC_FILTERS =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection AND r.function.metric = :metric "
          + "AND ((r.function.filters = :filters) or (r.function.filters is NULL and :filters is NULL)) "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_TIME_AND_FUNCTION_ID =
      "SELECT r FROM AnomalyResult r WHERE r.function.id = :functionId "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_VALID_BY_TIME_EMAIL_ID =
      "SELECT r FROM EmailConfiguration d JOIN d.functions f, AnomalyResult r "
          + "WHERE r.function.id=f.id AND d.id = :emailId AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc)) "
          + "AND r.dataMissing=:dataMissing";

  private static final String COUNT_GROUP_BY_COLLECTION_METRIC_DIMENSION =
      "select count(r.id) as num, r.function.collection, "
          + "r.function.metric, r.dimensions from AnomalyResult r "
          + "where r.function.isActive=true "
          + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
          + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc)) "
          + "group by r.function.collection, r.function.metric, r.dimensions "
          + "order by r.function.collection, num desc";

  private static final String COUNT_UNMERGED_BY_COLLECTION_METRIC_DIMENSION =
      "select count(r.id) as num, r.function.collection, "
          + "r.function.metric, r.dimensions from AnomalyResult r "
          + "where r.function.isActive=true and r.mergedResultId is null "
          + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
          + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc)) "
          + "group by r.function.collection, r.function.metric, r.dimensions "
          + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_FUNCTION = "select count(r.id) as num, r.function.id,"
      + "r.function.functionName, r.function.collection, r.function.metric from AnomalyResult r "
      + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric "
      + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_FUNCTION_DIMENSIONS =
      "select count(r.id) as num, r.function.id,"
          + "r.function.functionName, r.function.collection, r.function.metric, r.dimensions from AnomalyResult r "
          + "where r.function.isActive=true "
          + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
          + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
          + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric, r.dimensions "
          + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_COLLECTION_METRIC = "select count(r.id) as num, "
      + "r.function.collection, r.function.metric from AnomalyResult r "
      + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.collection, r.function.metric  "
      + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_COLLECTION = "select count(r.id) as num, "
      + "r.function.collection from AnomalyResult r " + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.collection order by r.function.collection, num desc";

  private static final String FIND_UNMERGED_BY_COLLECTION_METRIC_DIMENSION =
      "from AnomalyResult r where r.function.collection = :collection and r.function.metric = :metric "
          + "and r.dimensions=:dimensions and r.merged=false and r.dataMissing=:dataMissing";

  private static final String FIND_UNMERGED_BY_FUNCTION =
      "select r from AnomalyResult r where r.function.id = :functionId and r.merged=false "
          + "and r.dataMissing=:dataMissing";

  public AnomalyResultDAO() {
    super(AnomalyResult.class);
  }

  public List<AnomalyResult> findAllByCollectionAndTime(String collection, DateTime startTime,
      DateTime endTime) {
    //    getEntityManager().createQuery(FIND_BY_COLLECTION_TIME, entityClass);
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("collection", collection);
    parameterMap.put("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis());
    return super.executeParameterizedSQL(FIND_BY_COLLECTION_TIME, parameterMap);
  }

  public List<AnomalyResult> findAllByCollectionTimeAndMetric(String collection, String metric,
      DateTime startTime, DateTime endTime) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("collection", collection);
    parameterMap.put("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("metric", metric);
    return super.executeParameterizedSQL(FIND_BY_COLLECTION_TIME_METRIC, parameterMap);
  }

  public List<AnomalyResult> findAllByCollectionTimeMetricAndDimensions(String collection,
      String metric, DateTime startTime, DateTime endTime, String[] dimensions) {
    List<String> dimList = Arrays.asList(dimensions);
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("collection", collection);
    parameterMap.put("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("metric", metric);
    parameterMap.put("dimensions", dimList);
    return super.executeParameterizedSQL(FIND_BY_COLLECTION_TIME_METRIC_DIMENSION, parameterMap);

  }

  public List<AnomalyResult> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("startTimeUtc", startTime);
    parameterMap.put("endTimeUtc", endTime);
    parameterMap.put("functionId", functionId);
    return super.executeParameterizedSQL(FIND_BY_TIME_AND_FUNCTION_ID, parameterMap);
  }

  public List<AnomalyResult> findAllByCollectionTimeAndFilters(String collection,
      DateTime startTime, DateTime endTime, String filters) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("collection", collection);
    parameterMap.put("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("filters", filters);
    return super.executeParameterizedSQL(FIND_BY_COLLECTION_TIME_FILTERS, parameterMap);
  }

  public List<AnomalyResult> findAllByCollectionTimeMetricAndFilters(String collection,
      String metric, DateTime startTime, DateTime endTime, String filters) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("collection", collection);
    parameterMap.put("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("metric", metric);
    parameterMap.put("filters", filters);
    return super.executeParameterizedSQL(FIND_BY_COLLECTION_TIME_METRIC_FILTERS, parameterMap);
  }

  public List<AnomalyResult> findValidAllByTimeAndEmailId(DateTime startTime, DateTime endTime,
      long emailId) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis());
    parameterMap.put("emailId", emailId);
    parameterMap.put("dataMissing", false);
    return super.executeParameterizedSQL(FIND_VALID_BY_TIME_EMAIL_ID, parameterMap);
  }



  public List<AnomalyResult> findUnmergedByFunctionId(Long functionId) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionId", functionId);
    parameterMap.put("dataMissing", false);
    return super.executeParameterizedSQL(FIND_UNMERGED_BY_FUNCTION, parameterMap);
  }

  public List<AnomalyResult> findUnmergedByCollectionMetricAndDimensions(String collection,
      String metric, String dimensions) {
    Map<String, Object> parameterMap = new HashMap<>();

    parameterMap.put("collection", collection);
    parameterMap.put("metric", metric);
    parameterMap.put("dimensions", dimensions);
    parameterMap.put("dataMissing", false);
    return super.executeParameterizedSQL(FIND_UNMERGED_BY_COLLECTION_METRIC_DIMENSION,
        parameterMap);
  }
}
