package com.jzo2o.orders.manager.handler;


import cn.hutool.db.sql.Order;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.jzo2o.api.trade.RefundRecordApi;
import com.jzo2o.api.trade.dto.response.ExecutionResultResDTO;
import com.jzo2o.common.constants.UserType;
import com.jzo2o.common.utils.BeanUtils;
import com.jzo2o.common.utils.ObjectUtils;
import com.jzo2o.orders.base.enums.OrderRefundStatusEnum;
import com.jzo2o.orders.base.mapper.OrdersMapper;
import com.jzo2o.orders.base.mapper.OrdersRefundMapper;
import com.jzo2o.orders.base.model.domain.Orders;
import com.jzo2o.orders.base.model.domain.OrdersRefund;
import com.jzo2o.orders.manager.model.dto.OrderCancelDTO;
import com.jzo2o.orders.manager.service.IOrdersCreateService;
import com.jzo2o.orders.manager.service.IOrdersManagerService;
import com.jzo2o.orders.manager.service.IOrdersRefundService;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Component
public class OrdersHandler {


    @Resource
    private IOrdersCreateService OrdersCreateService;
    @Resource
    private IOrdersManagerService ordersManagerService;
    @Resource
    private IOrdersRefundService ordersRefundService;
    @Resource
    private RefundRecordApi refundRecordApi;
    @Resource
    private OrdersHandler ordersHandler;
    @Resource
    private OrdersMapper ordersMapper;

    //取消订单定时任务
    @XxlJob(value = "cancelOverTimePayOrder")
    public void cancelOverTimePayOrder(){
        List<Orders> orders = OrdersCreateService.queryOverTimePayOrdersListByCount(100);
        if (ObjectUtils.isEmpty(orders)){
            log.info("未查询到超时订单信息");
            return ;
        }
        for (Orders order : orders) {
            OrderCancelDTO orderCancelDTO = BeanUtils.toBean(order,OrderCancelDTO.class);
            orderCancelDTO.setCurrentUserType(UserType.SYSTEM);
            orderCancelDTO.setCancelReason("订单超时自动取消");
            ordersManagerService.cancel(orderCancelDTO);
        }
    }

    //退款定时任务
    @XxlJob(value = "handleRefundOrders")
    public void handleRefundOrders() {
        //查询orders_refund
        List<OrdersRefund> ordersRefunds = ordersRefundService.queryRefundOrderListByCount(100);
        //遍历退款记录，请求支付服务退款
        for (OrdersRefund ordersRefund : ordersRefunds) {
            requestRefundOrder(ordersRefund);
        }
    }

    public void requestRefundOrder(OrdersRefund ordersRefund) {
        //调用三方接口进行退款
        ExecutionResultResDTO executionResultResDTO=new ExecutionResultResDTO();
        try {
            executionResultResDTO=refundRecordApi.refundTrading(ordersRefund.getId(),ordersRefund.getRealPayAmount());
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(executionResultResDTO != null){
            //若退款成功则更改订单状态orders_status
            ordersHandler.refundOrder(ordersRefund,executionResultResDTO);
        }

    }

    public void refundOrder(OrdersRefund ordersRefund, ExecutionResultResDTO executionResultResDTO) {
        //此时有两种情况：退款成功 和 退款失败
        //定义一个状态变量，以备后续复用
        int status = OrderRefundStatusEnum.REFUNDING.getStatus();
        if(executionResultResDTO.getRefundStatus() ==  OrderRefundStatusEnum.REFUND_FAIL.getStatus()){
            //退款失败
            status=OrderRefundStatusEnum.REFUND_FAIL.getStatus();
        }else if(executionResultResDTO.getRefundStatus() ==  OrderRefundStatusEnum.REFUND_SUCCESS.getStatus()){
            status=OrderRefundStatusEnum.REFUND_SUCCESS.getStatus();
        }
        //复用前面变量
        if(status ==  OrderRefundStatusEnum.REFUNDING.getStatus()){
            return ;
        }
        LambdaUpdateWrapper<Orders> updateWrapper=new LambdaUpdateWrapper<Orders>()
                .eq(Orders::getId,ordersRefund.getId())
                .ne(Orders::getRefundStatus,status)
                .set(Orders::getRefundStatus,status)
                .set(ObjectUtils.isNotEmpty(executionResultResDTO.getRefundId()),Orders::getRefundId, executionResultResDTO.getRefundId())
                .set(ObjectUtils.isNotEmpty(executionResultResDTO.getRefundNo()),Orders::getRefundNo, executionResultResDTO.getRefundNo());
        int updated = ordersMapper.update(null, updateWrapper);
        if(updated > 0){
            //非退款中状态，删除申请退款记录，删除后定时任务不再扫描
            ordersRefundService.removeById(ordersRefund.getId());
        }
    }

    /**
     * 新起一个线程，即时退款
     */
    public void requestRefundNewThread(Long orderRefundId) {
        new Thread(()->{
            //根据订单Id查询orders_refund
            OrdersRefund ordersRefund = ordersRefundService.getById(orderRefundId);
            if (ObjectUtils.isNotNull(ordersRefund)) {
                //请求支付服务
                requestRefundOrder(ordersRefund);
            }
        }).start();
    }
}
