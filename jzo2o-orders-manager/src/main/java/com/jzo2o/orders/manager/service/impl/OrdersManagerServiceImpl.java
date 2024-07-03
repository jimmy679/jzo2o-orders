package com.jzo2o.orders.manager.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.db.DbRuntimeException;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jzo2o.api.orders.dto.response.OrderResDTO;
import com.jzo2o.api.orders.dto.response.OrderSimpleResDTO;
import com.jzo2o.api.trade.enums.PayChannelEnum;
import com.jzo2o.common.constants.UserType;
import com.jzo2o.common.enums.EnableStatusEnum;
import com.jzo2o.common.expcetions.CommonException;
import com.jzo2o.common.utils.BeanUtils;
import com.jzo2o.common.utils.ObjectUtils;
import com.jzo2o.orders.base.enums.OrderPayStatusEnum;
import com.jzo2o.orders.base.enums.OrderStatusEnum;
import com.jzo2o.orders.base.mapper.OrdersMapper;
import com.jzo2o.orders.base.model.domain.Orders;
import com.jzo2o.orders.base.model.domain.OrdersCanceled;
import com.jzo2o.orders.base.model.domain.OrdersRefund;
import com.jzo2o.orders.base.model.dto.OrderSnapshotDTO;
import com.jzo2o.orders.base.model.dto.OrderUpdateStatusDTO;
import com.jzo2o.orders.base.service.IOrdersCommonService;
import com.jzo2o.orders.manager.model.dto.OrderCancelDTO;
import com.jzo2o.orders.manager.model.dto.response.OrdersPayResDTO;
import com.jzo2o.orders.manager.service.IOrdersCanceledService;
import com.jzo2o.orders.manager.service.IOrdersCreateService;
import com.jzo2o.orders.manager.service.IOrdersManagerService;
import com.jzo2o.orders.manager.service.IOrdersRefundService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

import static com.jzo2o.orders.base.constants.FieldConstants.SORT_BY;

/**
 * <p>
 * 订单表 服务实现类
 * </p>
 *
 * @author itcast
 * @since 2023-07-10
 */
@Slf4j
@Service
public class OrdersManagerServiceImpl extends ServiceImpl<OrdersMapper, Orders> implements IOrdersManagerService {

    @Resource
    private IOrdersCanceledService ordersCanceledService;
    @Resource
    private IOrdersCommonService ordersCommonService;
    @Resource
    private OrdersManagerServiceImpl ordersManagerServiceImpl;
    @Resource
    private IOrdersCreateService ordersCreateService;
    @Resource
    private IOrdersRefundService ordersRefundService;
    @Override
    public List<Orders> batchQuery(List<Long> ids) {
        LambdaQueryWrapper<Orders> queryWrapper = Wrappers.<Orders>lambdaQuery().in(Orders::getId, ids).ge(Orders::getUserId, 0);
        return baseMapper.selectList(queryWrapper);
    }

    @Override
    public Orders queryById(Long id) {
        return baseMapper.selectById(id);
    }

    /**
     * 滚动分页查询
     *
     * @param currentUserId 当前用户id
     * @param ordersStatus  订单状态，0：待支付，100：派单中，200：待服务，300：服务中，400：待评价，500：订单完成，600：已取消，700：已关闭
     * @param sortBy        排序字段
     * @return 订单列表
     */
    @Override
    public List<OrderSimpleResDTO> consumerQueryList(Long currentUserId, Integer ordersStatus, Long sortBy) {
        //1.构件查询条件
        LambdaQueryWrapper<Orders> queryWrapper = Wrappers.<Orders>lambdaQuery()
                .eq(ObjectUtils.isNotNull(ordersStatus), Orders::getOrdersStatus, ordersStatus)
                .lt(ObjectUtils.isNotNull(sortBy), Orders::getSortBy, sortBy)
                .eq(Orders::getUserId, currentUserId)
                .eq(Orders::getDisplay, EnableStatusEnum.ENABLE.getStatus());
        Page<Orders> queryPage = new Page<>();
        queryPage.addOrder(OrderItem.desc(SORT_BY));
        queryPage.setSearchCount(false);

        //2.查询订单列表
        Page<Orders> ordersPage = baseMapper.selectPage(queryPage, queryWrapper);
        List<Orders> records = ordersPage.getRecords();
        List<OrderSimpleResDTO> orderSimpleResDTOS = BeanUtil.copyToList(records, OrderSimpleResDTO.class);
        return orderSimpleResDTOS;

    }
    /**
     * 根据订单id查询
     *
     * @param id 订单id
     * @return 订单详情
     */
    @Override
    public OrderResDTO getDetail(Long id) {
        Orders orders = queryById(id);
        //懒加载方式取消支付超时订单
        orders = canalIfPayOvertime(orders);
        //需要在返回前执行此步骤，因为下一步即将进行结果封装返回
        OrderResDTO orderResDTO = BeanUtil.toBean(orders, OrderResDTO.class);
        return orderResDTO;
    }
    //懒加载方式取消支付超时订单
    public Orders canalIfPayOvertime(Orders orders){
        if(orders.getOrdersStatus() == OrderStatusEnum.NO_PAY.getStatus() && orders.getCreateTime().plusMinutes(15).isBefore(LocalDateTime.now())){
            //查询最新支付状态，看是否是未支付
            OrdersPayResDTO payResultFromTradServer = ordersCreateService.getPayResultFromTradServer(orders.getId());
            if(ObjectUtils.isNotEmpty(payResultFromTradServer) && payResultFromTradServer.getPayStatus()!= OrderPayStatusEnum.PAY_SUCCESS.getStatus()){
                //修改订单状态为取消
                OrderCancelDTO orderCancelDTO = BeanUtil.toBean(orders, OrderCancelDTO.class);
                orderCancelDTO.setCurrentUserType(UserType.SYSTEM);
                orderCancelDTO.setCancelReason("订单超时支付，自动取消");
                cancel(orderCancelDTO);
                orders = getById(orders.getId());
            }
        }
        return orders;
    }



    /**
     * 订单评价
     *
     * @param ordersId 订单id
     */
    @Override
    @Transactional
    public void evaluationOrder(Long ordersId) {
//        //查询订单详情
//        Orders orders = queryById(ordersId);
//
//        //构建订单快照
//        OrderSnapshotDTO orderSnapshotDTO = OrderSnapshotDTO.builder()
//                .evaluationTime(LocalDateTime.now())
//                .build();
//
//        //订单状态变更
//        orderStateMachine.changeStatus(orders.getUserId(), orders.getId().toString(), OrderStatusChangeEventEnum.EVALUATE, orderSnapshotDTO);
    }

    @Override
    public void cancel(OrderCancelDTO orderCancelDTO) {
        //判断订单是否存在
        Orders orders = getById(orderCancelDTO.getId());
        if (ObjectUtil.isNull(orders)) {
            throw new DbRuntimeException("找不到要取消的订单,订单号：{}",orderCancelDTO.getId());
        }
        //订单状态
        Integer ordersStatus = orders.getOrdersStatus();
        //根据订单状态执行取消逻辑
        if(OrderStatusEnum.NO_PAY.getStatus()==ordersStatus){
            //订单状态为待支付
            //抽取方法
            ordersManagerServiceImpl.cancelByNoPay(orderCancelDTO);
        }else if(OrderStatusEnum.DISPATCHING.getStatus()==ordersStatus){
            //订单状态为派单中
            //向数据库保存三条记录：orders_canceled、orders_refund、orders
            //抽取单独方法
            ordersManagerServiceImpl.cancelByDispatching(orderCancelDTO);
        }else{
            throw new CommonException("当前订单状态不支持取消");
        }
    }


    //未支付状态取消订单方法
    @Transactional(rollbackFor = Exception.class)
    public void cancelByNoPay(OrderCancelDTO orderCancelDTO) {
        //保存取消订单记录
        OrdersCanceled ordersCanceled = BeanUtil.toBean(orderCancelDTO, OrdersCanceled.class);
        ordersCanceled.setCancellerId(orderCancelDTO.getCurrentUserId());
        ordersCanceled.setCancelerName(orderCancelDTO.getCurrentUserName());
        ordersCanceled.setCancellerType(orderCancelDTO.getCurrentUserType());
        ordersCanceled.setCancelTime(LocalDateTime.now());
        ordersCanceledService.save(ordersCanceled);
        //更新订单状态为取消订单
        OrderUpdateStatusDTO orderUpdateStatusDTO = OrderUpdateStatusDTO.builder()
                .id(orderCancelDTO.getId())
                .originStatus(OrderStatusEnum.NO_PAY.getStatus())
                .targetStatus(OrderStatusEnum.CANCELED.getStatus())
                .build();
        int result = ordersCommonService.updateStatus(orderUpdateStatusDTO);
        if (result <= 0) {
            throw new DbRuntimeException("订单取消事件处理失败");
        }
    }


    //取消派单中订单方法
    @Transactional(rollbackFor = Exception.class)
    public void cancelByDispatching(OrderCancelDTO orderCancelDTO){
        //向数据库保存三条记录：orders_canceled、orders_refund、orders
        //保存取消订单记录
        OrdersCanceled ordersCanceled = BeanUtil.toBean(orderCancelDTO, OrdersCanceled.class);
        ordersCanceled.setCancellerId(orderCancelDTO.getCurrentUserId());
        ordersCanceled.setCancelerName(orderCancelDTO.getCurrentUserName());
        ordersCanceled.setCancellerType(orderCancelDTO.getCurrentUserType());
        ordersCanceled.setCancelTime(LocalDateTime.now());
        ordersCanceledService.save(ordersCanceled);
        //更新订单状态为关闭订单
        //update orders set orders_status = 700 where id = ? and orders_status = 100;
        OrderUpdateStatusDTO orderUpdateStatusDTO = OrderUpdateStatusDTO.builder()
                .id(orderCancelDTO.getId())
                .originStatus(OrderStatusEnum.DISPATCHING.getStatus())
                .targetStatus(OrderStatusEnum.CLOSED.getStatus())
                .build();
        int result = ordersCommonService.updateStatus(orderUpdateStatusDTO);
        if (result <= 0) {
            throw new DbRuntimeException("订单取消事件处理失败");
        }
        //保存退款记录
        OrdersRefund ordersRefund= BeanUtils.toBean(orderCancelDTO,OrdersRefund.class);
        boolean save = ordersRefundService.save(ordersRefund);
        if (!save){
            log.info("保存派单中订单退款记录失败！");
        }
    }
}
