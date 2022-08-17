package com.zdjizhi.utils.functions;

import com.zdjizhi.pojo.Entity;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterNullFunction implements FilterFunction<Entity> {

    @Override
    public boolean filter(Entity entity) {

        return entity.getIfError()!=1;
    }
}

