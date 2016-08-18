package com.linkedin.thirdeye.client.pinot.slsummary;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


public class Record {
  public String dimensionName;
  public String dimensionValue;
  public double metricA;
  public double metricB;

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
