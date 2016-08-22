package com.linkedin.thirdeye.dbi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.modelmapper.ModelMapper;
import org.modelmapper.convention.NameTokenizers;

import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;

public class GenericResultSetMapper {

  ModelMapper modelMapper = new ModelMapper();

  {
    modelMapper.getConfiguration().setSourceNameTokenizer(NameTokenizers.UNDERSCORE);
  }

  public AbstractBaseEntity mapSingle(ResultSet rs, Class<? extends AbstractBaseEntity> entityClass)
      throws Exception {
    List<Map<String, Object>> resultMapList = toResultMapList(rs);
    if (resultMapList.size() > 0) {
      Map<String, Object> map = resultMapList.get(0);
      AbstractBaseEntity entity = modelMapper.map(map, entityClass);
      return entity;
    }
    return null;
  }

  public List<AbstractBaseEntity> mapAll(ResultSet rs,
      Class<? extends AbstractBaseEntity> entityClass) throws Exception {
    List<Map<String, Object>> resultMapList = toResultMapList(rs);
    List<AbstractBaseEntity> resultEntityList = new ArrayList<>();
    if (resultMapList.size() > 0) {
      for (Map<String, Object> map : resultMapList) {
        AbstractBaseEntity entity = modelMapper.map(map, entityClass);
        resultEntityList.add(entity);
      }
      return resultEntityList;
    }
    return null;
  }

  List<Map<String, Object>> toResultMapList(ResultSet rs) throws Exception {
    List<Map<String, Object>> resultMapList = new ArrayList<>();
    while (rs.next()) {
      ResultSetMetaData resultSetMetaData = rs.getMetaData();
      int numColumns = resultSetMetaData.getColumnCount();
      HashMap<String, Object> map = new HashMap<>();
      for (int i = 1; i <= numColumns; i++) {
        map.put(resultSetMetaData.getColumnLabel(i), rs.getObject(i));
      }
      resultMapList.add(map);
    }
    return resultMapList;
  }
}
