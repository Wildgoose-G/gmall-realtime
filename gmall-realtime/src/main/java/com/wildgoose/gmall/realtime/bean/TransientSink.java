package com.wildgoose.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author 张翔
 * @date 2023/3/5 11:39
 * @description
 */
@Target(ElementType.FIELD) // 作用域--字段
@Retention(RetentionPolicy.RUNTIME) // 生效时机--运行时生效
public @interface TransientSink {
}
