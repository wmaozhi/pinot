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

  @SuppressWarnings("unchecked")
  public <E extends AbstractBaseEntity> E mapSingle(ResultSet rs, Class<E> entityClass) throws Exception {
    List<Map<String, Object>> resultMapList = toResultMapList(rs);
    if (resultMapList.size() > 0) {
      Map<String, Object> map = resultMapList.get(0);
      AbstractBaseEntity entity = modelMapper.map(map, entityClass);
      return (E) entity;
    }
    return null;
  }

  <E extends AbstractBaseEntity> List<E> mapAll(ResultSet rs, AbstractBaseEntity E) {

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
