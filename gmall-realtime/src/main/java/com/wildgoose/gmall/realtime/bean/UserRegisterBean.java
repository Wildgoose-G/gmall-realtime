package com.wildgoose.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author 张翔
 * @date 2023/3/7 16:09
 * @description
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}
