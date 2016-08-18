package com.linkedin.thirdeye.client.pinot.summary;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;


/**
 * Provide low-level operation of query to Pinot.
 */
public class PinotThirdEyeSummaryClient implements OLAPDataBaseClient {
  private final static DateTime NULL_DATETIME = new DateTime();
  private final static TimeGranularity NULL_TIME_GRANULARITY = new TimeGranularity(0, TimeUnit.HOURS);

  QueryCache queryCache;
  String collection;
  List<MetricFunction> metricFunctions = new ArrayList<>();
  TimeGranularity timeGranularity = NULL_TIME_GRANULARITY;
  DateTime baselineStartInclusive = NULL_DATETIME;
  DateTime baselineEndExclusive = NULL_DATETIME;
  DateTime currentStartInclusive = NULL_DATETIME;
  DateTime currentEndExclusive = NULL_DATETIME;

  public PinotThirdEyeSummaryClient(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public PinotThirdEyeSummaryClient(ThirdEyeClient thirdEyeClient) {
    this(new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10)));
  }

  @Override
  public void setCollection(String collection) {
    this.collection = collection;
  }

  @Override
  public void setGroupByTimeGranularity(TimeGranularity timeGranularity) {
    if (!this.timeGranularity.equals(timeGranularity)) {
      this.timeGranularity = timeGranularity;
      updateBaselineEndExclusive();
      updateCurrentEndExclusive();
    }
  }

  @Override
  public void setBaselineStartInclusive(DateTime dateTime) {
    if (!this.baselineStartInclusive.equals(dateTime)) {
      baselineStartInclusive = dateTime;
      updateBaselineEndExclusive();
    }
  }

  @Override
  public void setCurrentStartInclusive(DateTime dateTime) {
    if (!this.currentStartInclusive.equals(dateTime)) {
      currentStartInclusive = dateTime;
      updateCurrentEndExclusive();
    }
  }

  private void updateBaselineEndExclusive() {
    baselineEndExclusive = baselineStartInclusive.plus(timeGranularity.toMillis());
  }

  private void updateCurrentEndExclusive() {
    currentEndExclusive = currentStartInclusive.plus(timeGranularity.toMillis());
  }

  @Override
  public void setMetricName(String metricName) {
    metricFunctions.clear();
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, metricName));
  }

  @Override
  public Row getTopAggregatedValues() {
    List<ThirdEyeRequest> bulkRequests = new ArrayList<>();
    List<String> groupBy = Collections.emptyList();
    Pair<ThirdEyeRequest, ThirdEyeRequest> timeOnTimeRequests = constructTimeOnTimeRequest(groupBy);
    bulkRequests.add(timeOnTimeRequests.getLeft());
    bulkRequests.add(timeOnTimeRequests.getRight());

    return constructMultiLevelAggregatedValues(null, bulkRequests).get(0).get(0);
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions) {
    List<ThirdEyeRequest> bulkRequests = new ArrayList<>();
    for (int level = 0; level < dimensions.size(); ++level) {
      List<String> groupBy = Lists.newArrayList(dimensions.get(level));
      Pair<ThirdEyeRequest, ThirdEyeRequest> timeOnTimeRequests = constructTimeOnTimeRequest(groupBy);
      bulkRequests.add(timeOnTimeRequests.getLeft());
      bulkRequests.add(timeOnTimeRequests.getRight());
    }

    return constructMultiLevelAggregatedValues(dimensions, bulkRequests);
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions) {
    List<ThirdEyeRequest> bulkRequests = new ArrayList<>();
    for (int level = 0; level < dimensions.size() + 1; ++level) {
      List<String> groupBy = new ArrayList<>(dimensions.groupByStringsAtLevel(level));
      Pair<ThirdEyeRequest, ThirdEyeRequest> timeOnTimeRequests = constructTimeOnTimeRequest(groupBy);
      bulkRequests.add(timeOnTimeRequests.getLeft());
      bulkRequests.add(timeOnTimeRequests.getRight());
    }

    return constructMultiLevelAggregatedValues(dimensions, bulkRequests);
  }

  private List<List<Row>> constructMultiLevelAggregatedValues(Dimensions dimensions, List<ThirdEyeRequest> bulkRequests) {
    List<List<Row>> res = new ArrayList<>();

    try {
      Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResponses = queryCache.getQueryResultsAsync(bulkRequests);
      for (int i = 0; i < bulkRequests.size(); i += 2) {
        ThirdEyeResponse responseTa = queryResponses.get(bulkRequests.get(i)).get();
        ThirdEyeResponse responseTb = queryResponses.get(bulkRequests.get(i+1)).get();
        List<Row> singleLevelRows = constructSingleLevelAggregatedValues(dimensions, responseTa, responseTb);
        res.add(singleLevelRows);
      }
    } catch (Exception e) {
      e.printStackTrace();
      res.clear();
    }

    return res;
  }

  private List<Row> constructSingleLevelAggregatedValues(Dimensions dimensions, ThirdEyeResponse responseTa, ThirdEyeResponse responseTb) {
    List<Row> rows = new ArrayList<>();
    // TODO: (Performance) Replace the key: List<String>
    Map<List<String>, Integer> rowTable = new HashMap<>();
    {
      for (int j = 0; j < responseTa.getNumRows(); ++j) {
        List<String> dimensionValues = responseTa.getRow(j).getDimensions();
        double value = responseTa.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          Row row = new Row();
          row.dimensions = dimensions;
          row.dimensionValues = new DimensionValues(dimensionValues);
          row.baselineValue = value;
          rowTable.put(dimensionValues, rows.size());
          rows.add(row);
        }
      }
    }

    {
      for (int j = 0; j < responseTb.getNumRows(); ++j) {
        List<String> dimensionValues = responseTb.getRow(j).getDimensions();
        double value = responseTb.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          if (rowTable.containsKey(dimensionValues)) {
            rows.get(rowTable.get(dimensionValues)).currentValue = value;
          } else {
            Row row = new Row();
            row.dimensions = dimensions;
            row.dimensionValues = new DimensionValues(dimensionValues);
            row.currentValue = value;
            rows.add(row);
          }
        }
      }
    }

    return rows;
  }

  private Pair<ThirdEyeRequest, ThirdEyeRequest> constructTimeOnTimeRequest(List<String> groupBy) {
    ThirdEyeRequest.ThirdEyeRequestBuilder builder = new ThirdEyeRequest.ThirdEyeRequestBuilder();
    builder.setCollection(collection);
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupByTimeGranularity(timeGranularity);
    builder.setGroupBy(groupBy);

    builder.setStartTimeInclusive(baselineStartInclusive);
    builder.setEndTimeExclusive(baselineEndExclusive);
    ThirdEyeRequest baselineRequest = builder.build("baseline");

    builder.setStartTimeInclusive(currentStartInclusive);
    builder.setEndTimeExclusive(currentEndExclusive);
    ThirdEyeRequest currentRequest = builder.build("current");

    return new ImmutablePair<ThirdEyeRequest, ThirdEyeRequest>(baselineRequest, currentRequest);
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] argc) throws Exception {
    String oFileName = "Cube.json";

    String collection = "thirdeyeKbmi";
    String metricName = "mobilePageViews";
    DateTime baselineStart = new DateTime(1469628000000L);
    DateTime currentStart = new DateTime(1470232800000L);
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);

//    String collection = "thirdeyeAbook";
//    String metricName = "totalFlows";
//    DateTime baselineStart = new DateTime(2016, 7, 11, 00, 00);
//    DateTime currentStart = new DateTime(2016, 7, 18, 00, 00);
//    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);

    String[] dimensionNames = { "browserName", "continent", "countryCode",
                                "deviceName", "environment", "locale", "osName",
                                "pageKey", "service", "sourceApp" };
    List<List<String>> hierarchies = new ArrayList<>();
    hierarchies.add(Lists.newArrayList("continent", "countryCode"));

    // Create ThirdEye client
    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeDashboardConfiguration();
    thirdEyeConfig.setWhitelistCollections(collection);

    PinotThirdEyeClientConfig pinotThirdEyeClientConfig = new PinotThirdEyeClientConfig();
    pinotThirdEyeClientConfig.setControllerHost("lva1-app0086.corp.linkedin.com");
    pinotThirdEyeClientConfig.setControllerPort(11984);
    pinotThirdEyeClientConfig.setZookeeperUrl("zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster");
    pinotThirdEyeClientConfig.setClusterName("mpSprintDemoCluster");

    ThirdEyeCacheRegistry.initializeWebappCaches(thirdEyeConfig, pinotThirdEyeClientConfig);

    OLAPDataBaseClient pinotClient = new PinotThirdEyeSummaryClient(ThirdEyeCacheRegistry.getInstance().getQueryCache());
    pinotClient.setCollection(collection);
    pinotClient.setMetricName(metricName);
    pinotClient.setGroupByTimeGranularity(timeGranularity);
    pinotClient.setBaselineStartInclusive(baselineStart);
    pinotClient.setCurrentStartInclusive(currentStart);

    int maxDimensionSize = 2;

    // Build the cube for computing the summary
    Cube initCube = new Cube();
    initCube.buildWithAutoDimensionOrder(pinotClient, new Dimensions(Lists.newArrayList(dimensionNames)),
        maxDimensionSize, hierarchies);
//    initCube.buildWithManualDimensionOrder(pinotClient, new Dimensions(Lists.newArrayList(dimensionNames)));


    int answerSize = 5;
    boolean oneSideErrors = false;
    Summary summary = new Summary(initCube);
    System.out.println(summary.computeSummary(answerSize, oneSideErrors, maxDimensionSize));

    try {
      initCube.toJson(oFileName);
      Cube cube = Cube.fromJson(oFileName);
      System.out.println("Restored Cube:");
      System.out.println(cube);
      summary = new Summary(cube);
      System.out.println(summary.computeSummary(answerSize, oneSideErrors, maxDimensionSize));
    } catch (IOException e) {
      System.err.println("WARN: Unable to save the cube to the file: " + oFileName);
      e.printStackTrace();
    }

    // closing
    System.exit(0);
  }
}
