package com.linkedin.thirdeye.dbi;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.jdbcx.JdbcDataSource;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.mysql.jdbc.Statement;


public class SqlQueryBuilder {

  //Map<TableName,EntityName>
  BiMap<String, String> tableToEntityNameMap = HashBiMap.create();
  Map<String, List<ColumnInfo>> columnInfoPerTable = new HashMap<>();
  //insert sql per table
  Map<String, String> insertSqlMap = new HashMap<>();

  public void register(Connection connection, Class<? extends AbstractBaseEntity> entityClass,
      String tableName) throws Exception {
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    String catalog = null;
    String schemaPattern = null;
    String tableNamePattern = tableName;
    String columnNamePattern = null;
    ResultSet rs =
        databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

    tableToEntityNameMap.put(tableName, entityClass.getSimpleName());
    Map<String, ColumnInfo> columnsInfoMap = new HashMap<>();
    List<ColumnInfo> columnInfoList = new ArrayList<>();

    while (rs.next()) {
      String columnName = rs.getString(4);
      ColumnInfo columnInfo = new ColumnInfo();
      columnInfo.columnNameInDB = columnName;
      columnInfo.sqlType = rs.getInt(5);
      columnsInfoMap.put(columnName, columnInfo);
      columnInfoList.add(columnInfo);
    }
    List<Field> fields = new ArrayList<>();
    getAllFields(fields, entityClass);
    for (Field field : fields) {
      field.setAccessible(true);
      String entityColumn = field.getName();
      for (String dbColumn : columnsInfoMap.keySet()) {
        boolean success = false;
        if (dbColumn.toLowerCase().equals(entityColumn.toLowerCase())) {
          success = true;
        }
        String dbColumnNormalized = dbColumn.replaceAll("_", "").toLowerCase();
        String entityColumnNormalized = entityColumn.replaceAll("_", "").toLowerCase();
        if (dbColumnNormalized.equals(entityColumnNormalized)) {
          success = true;
        }
        if (success) {
          columnsInfoMap.get(dbColumn).columnNameInEntity = entityColumn;
          columnsInfoMap.get(dbColumn).field = field;
          System.out.println("Mapped " + dbColumn + " to " + entityColumn);
        }
      }
    }
    columnInfoPerTable.put(tableName, columnInfoList);
    //create insert sql for this table

    String insertSql = generateInsertSql(tableName, columnInfoList);
    insertSqlMap.put(tableName, insertSql);
    System.out.println(insertSql);
  }

  public static String generateInsertSql(String tableName, List<ColumnInfo> columnInfoList) {

    StringBuilder values = new StringBuilder(" VALUES");
    StringBuilder names = new StringBuilder("");
    names.append("(");
    values.append("(");
    String delim = "";
    for (ColumnInfo columnInfo : columnInfoList) {
      String columnName = columnInfo.columnNameInDB;
      if (!columnName.toLowerCase().equals("id")) {
        names.append(delim);
        names.append(columnName);
        values.append(delim);
        values.append("?");
        delim = ",";
      }
    }
    names.append(")");
    values.append(")");

    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(tableName).append(names.toString()).append(values.toString());
    return sb.toString();
  }

  public PreparedStatement createInsertStatement(Connection conn, AbstractBaseEntity entity)
      throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    return createInsertStatement(conn, tableName, entity);
  }

  public PreparedStatement createInsertStatement(Connection conn, String tableName,
      AbstractBaseEntity entity) throws Exception {

    String sql = insertSqlMap.get(tableName);
    PreparedStatement preparedStatement =
        conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    List<ColumnInfo> columnInfoList = columnInfoPerTable.get(tableName);
    int parameterIndex = 1;
    for (int i = 0; i < columnInfoList.size(); i++) {
      ColumnInfo columnInfo = columnInfoList.get(i);
      if (!columnInfo.columnNameInDB.toLowerCase().equals("id")) {
        Object val = columnInfo.field.get(entity);
        System.out.println("Setting value:" + val + " for " + columnInfo.columnNameInDB);
        preparedStatement.setObject(parameterIndex++, val.toString(), columnInfo.sqlType);
      }
    }
    return preparedStatement;
  }

  public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
    fields.addAll(Arrays.asList(type.getDeclaredFields()));

    if (type.getSuperclass() != null) {
      fields = getAllFields(fields, type.getSuperclass());
    }

    return fields;
  }


  public static void main(String[] args) throws Exception {
    JdbcDataSource ds = new JdbcDataSource();
    ds.setURL("jdbc:h2:mem:test");
    ds.setUser("sa");
    ds.setPassword("sa");
    Connection conn = ds.getConnection();
    conn.setAutoCommit(true);
    conn.createStatement().execute(
        "Create table anomaly_feedback (id bigint, feedback_type varchar(150), comment varchar(500), status varchar(200))");
    ResultSet resultSet = conn.getMetaData().getTables(null, null, null, null);
    while (resultSet.next()) {
      System.out.println(resultSet.getString(3));
    }
    SqlQueryBuilder builder = new SqlQueryBuilder();
    builder.register(conn, AnomalyFeedback.class, "ANOMALY_FEEDBACK");

  }

  class ColumnInfo {
    String columnNameInDB;
    int sqlType;
    String columnNameInEntity;
    Field field;
  }

  public PreparedStatement createFindByIdStatement(Connection connection,
      Class<? extends AbstractBaseEntity> entityClass, Long id) throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Select * from " + tableName + " where id=?";
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    prepareStatement.setLong(1, id);
    return prepareStatement;
  }

}
