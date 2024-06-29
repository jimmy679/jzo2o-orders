package com.jzo2o.orders.manager.listener;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson.JSON;
import com.jzo2o.api.trade.enums.TradingStateEnum;
import com.jzo2o.common.constants.MqConstants;
import com.jzo2o.common.model.msg.TradeStatusMsg;
import com.jzo2o.orders.manager.service.IOrdersCreateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TradeStatusListener {
    @Resource
    private IOrdersCreateService ordersCreateService;

    /**
     * 更新支付结果
     * 支付成功
     *
     * @param msg 消息
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MqConstants.Queues.ORDERS_TRADE_UPDATE_STATUS),
            exchange = @Exchange(name = MqConstants.Exchanges.TRADE, type = ExchangeTypes.TOPIC),
            key = MqConstants.RoutingKeys.TRADE_UPDATE_STATUS
    ))
    public void listenTradeUpdatePayStatusMsg(String msg) {
        log.info("接收到支付结果状态的消息 ({})-> {}", MqConstants.Queues.ORDERS_TRADE_UPDATE_STATUS, msg);
        //通过查阅支付服务源码：String msg = JSONUtil.toJsonStr(tradeMsgList);
        //        rabbitClient.sendMsg(MqConstants.Exchanges.TRADE, MqConstants.RoutingKeys.TRADE_UPDATE_STATUS, msg);
        //发现，传入的消息对象是List<TradeStatusMsg>格式，于是下面
        //解析消息：将msg转为java对象
        List<TradeStatusMsg> tradeStatusMsgList = JSON.parseArray(msg, TradeStatusMsg.class);

        //只处理家政服务的订单(appId=="jzo2o.orders")且是支付成功的(status==4)
        List<TradeStatusMsg> msgList = tradeStatusMsgList.stream()
                .filter(v ->
                        v.getStatusCode().equals(TradingStateEnum.YJS.getCode()) &&
                        "jzo2o.orders".equals(v.getProductAppId()))
                .collect(Collectors.toList());
        if (CollUtil.isEmpty(msgList)) {
            return;
        }

        //修改订单状态
        msgList.forEach(m -> ordersCreateService.paySuccess(m));
    }
}