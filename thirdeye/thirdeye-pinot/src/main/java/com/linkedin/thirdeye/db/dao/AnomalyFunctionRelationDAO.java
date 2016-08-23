package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;

public class AnomalyFunctionRelationDAO extends AbstractBaseDAO<AnomalyFunctionRelation> {
  public AnomalyFunctionRelationDAO() {
    super(AnomalyFunctionRelation.class);
  }

  public void deleteByParent(Long parentId) {
    super.deleteById(parentId);
  }

  public void deleteByParentChild(Long parentId, Long childId) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("parentId", parentId);
    filters.put("childId", childId);
    super.deleteByParams(filters);
  }

  public List<AnomalyFunctionRelation> findByParent(Long parentId) {
    return super.findByParams(ImmutableMap.of("parentId", parentId));
  }
}
