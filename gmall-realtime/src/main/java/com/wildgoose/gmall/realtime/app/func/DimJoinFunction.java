package com.wildgoose.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author 张翔
 * @date 2023/3/8 16:52
 * @description
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    //获取维度主键的方法
    String getKey(T obj);
}
