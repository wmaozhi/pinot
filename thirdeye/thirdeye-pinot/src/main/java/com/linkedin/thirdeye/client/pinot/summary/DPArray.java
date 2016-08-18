package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


public class DPArray {
  double targetRatio;
  List<DPSlot> slots;

  public DPArray(int N) {
    slots = new ArrayList<>(N + 1);
    for (int i = 0; i <= N; ++i) {
      slots.add(new DPSlot());
    }
  }

  public DPSlot get(int index) {
    return slots.get(index);
  }

  public int size() {
    return slots.size();
  }

  public Set<HierarchyNode> getAnswer() {
    return new HashSet<>(slots.get(slots.size() - 1).ans);
  }

  public String toString() {
    if (slots != null) {
      StringBuilder sb = new StringBuilder();
      for (DPSlot slot : slots) {
        sb.append(ToStringBuilder.reflectionToString(slot, ToStringStyle.SHORT_PREFIX_STYLE));
        sb.append('\n');
      }
      return sb.toString();
    } else
      return "";
  }

  public static class DPSlot {
    Set<HierarchyNode> ans = new HashSet<>();
    double cost;

    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }
}
