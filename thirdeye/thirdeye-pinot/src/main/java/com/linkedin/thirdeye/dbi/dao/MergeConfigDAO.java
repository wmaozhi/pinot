package com.linkedin.thirdeye.dbi.dao;

import com.linkedin.thirdeye.dbi.mapper.MergeConfigMapper;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;

@RegisterMapper(MergeConfigMapper.class)
public interface MergeConfigDAO {

}
