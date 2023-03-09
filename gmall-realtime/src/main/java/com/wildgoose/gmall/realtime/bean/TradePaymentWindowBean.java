package com.wildgoose.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author 张翔
 * @date 2023/3/7 16:36
 * @description
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;

    // 时间戳
    Long ts;
}
