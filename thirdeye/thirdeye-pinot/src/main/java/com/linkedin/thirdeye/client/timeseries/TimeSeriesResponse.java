package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;

public class TimeSeriesResponse {
  int numRows;
  private final Set<String> metrics = new TreeSet<>();
  private final Set<String> dimensions = new TreeSet<>();
  private final List<TimeSeriesRow> rows;

  public TimeSeriesResponse(List<TimeSeriesRow> rows) {
    this.rows = rows;
    for (TimeSeriesRow row : rows) {
      for (TimeSeriesMetric metric : row.getMetrics()) {
        metrics.add(metric.getMetricName());
      }
      dimensions.add(row.getDimensionName());
    }
    numRows = rows.size();
  }

  public int getNumRows() {
    return numRows;
  }

  public Set<String> getMetrics() {
    return metrics;
  }

  public Set<String> getDimensions() {
    return dimensions;
  }

  public TimeSeriesRow getRow(int index) {
    return rows.get(index);
  }

  public static class Builder {
    List<TimeSeriesRow> rows = new ArrayList<>();

    public void add(TimeSeriesRow row) {
      rows.add(row);
    }

    TimeSeriesResponse build() {
      return new TimeSeriesResponse(rows);
    }
  }
}
