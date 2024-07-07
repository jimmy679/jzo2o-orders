package com.jzo2o.orders.manager.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.db.DbRuntimeException;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jzo2o.api.orders.dto.response.OrderResDTO;
import com.jzo2o.api.orders.dto.response.OrderSimpleResDTO;
import com.jzo2o.api.trade.enums.PayChannelEnum;
import com.jzo2o.api.trade.enums.RefundStatusEnum;
import com.jzo2o.common.constants.UserType;
import com.jzo2o.common.enums.EnableStatusEnum;
import com.jzo2o.common.expcetions.CommonException;
import com.jzo2o.common.model.PageResult;
import com.jzo2o.common.utils.BeanUtils;
import com.jzo2o.common.utils.CollUtils;
import com.jzo2o.common.utils.JsonUtils;
import com.jzo2o.common.utils.ObjectUtils;
import com.jzo2o.mysql.utils.PageUtils;
import com.jzo2o.orders.base.config.OrderStateMachine;
import com.jzo2o.orders.base.enums.OrderPayStatusEnum;
import com.jzo2o.orders.base.enums.OrderRefundStatusEnum;
import com.jzo2o.orders.base.enums.OrderStatusChangeEventEnum;
import com.jzo2o.orders.base.enums.OrderStatusEnum;
import com.jzo2o.orders.base.mapper.OrdersMapper;
import com.jzo2o.orders.base.model.domain.Orders;
import com.jzo2o.orders.base.model.domain.OrdersCanceled;
import com.jzo2o.orders.base.model.domain.OrdersRefund;
import com.jzo2o.orders.base.model.dto.OrderSnapshotDTO;
import com.jzo2o.orders.base.model.dto.OrderUpdateStatusDTO;
import com.jzo2o.orders.base.service.IOrdersCommonService;
import com.jzo2o.orders.manager.handler.OrdersHandler;
import com.jzo2o.orders.manager.model.dto.OrderCancelDTO;
import com.jzo2o.orders.manager.model.dto.request.OrderPageQueryReqDTO;
import com.jzo2o.orders.manager.model.dto.response.OrdersPayResDTO;
import com.jzo2o.orders.manager.service.IOrdersCanceledService;
import com.jzo2o.orders.manager.service.IOrdersCreateService;
import com.jzo2o.orders.manager.service.IOrdersManagerService;
import com.jzo2o.orders.manager.service.IOrdersRefundService;
import com.jzo2o.redis.helper.CacheHelper;
import io.lettuce.core.output.ScanOutput;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.jzo2o.orders.base.constants.FieldConstants.SORT_BY;
import static com.jzo2o.orders.base.constants.RedisConstants.RedisKey.ORDERS;
import static com.jzo2o.orders.base.constants.RedisConstants.Ttl.ORDERS_PAGE_TTL;

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
    @Resource
    private OrdersHandler ordersHandler;
    @Resource
    private OrderStateMachine orderStateMachine;
    @Resource
    private CacheHelper cacheHelper;

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
        //构件查询条件
        LambdaQueryWrapper<Orders> queryWrapper = Wrappers.<Orders>lambdaQuery()
                .eq(ObjectUtils.isNotNull(ordersStatus), Orders::getOrdersStatus, ordersStatus)
                .lt(ObjectUtils.isNotNull(sortBy), Orders::getSortBy, sortBy)
                .eq(Orders::getUserId, currentUserId)
                .eq(Orders::getDisplay, EnableStatusEnum.ENABLE.getStatus())
                .select(Orders::getId);//只查询id列
        Page<Orders> queryPage = new Page<>();
        queryPage.addOrder(OrderItem.desc(SORT_BY));
        queryPage.setSearchCount(false);

        //查询订单id列表
        Page<Orders> ordersPage = baseMapper.selectPage(queryPage, queryWrapper);
        if (ObjectUtil.isEmpty(ordersPage.getRecords())) {
            return new ArrayList<>();
        }
        //提取订单id列表
        List<Long> ordersIds = CollUtils.getFieldValues(ordersPage.getRecords(), Orders::getId);

        //先查询缓存，缓存没有再查询数据库
        //参数1：redisKey的一部分
        String redisKey = String.format(ORDERS, currentUserId);
        //参数2：订单id列表
        //参数3：batchDataQueryExecutor 当缓存中没有时执行batchDataQueryExecutor从数据库查询
        // batchDataQueryExecutor的方法：Map<K, T> execute(List<K> objectIds, Class<T> clazz); objectIds表示缓存中未匹配到的id，clazz指定map中value的数据类型
        //参数4：返回List中的数据类型
        //参数5：过期时间
        List<OrderSimpleResDTO> orderSimpleResDTOS = cacheHelper.<Long, OrderSimpleResDTO>batchGet(redisKey, ordersIds, (noCacheIds, clazz) -> {
            List<Orders> ordersList = batchQuery(noCacheIds);
            if (CollUtils.isEmpty(ordersList)) {
                //为了防止缓存穿透返回空数据
                return new HashMap<>();
            }
            Map<Long, OrderSimpleResDTO> collect = ordersList.stream().collect(Collectors.toMap(Orders::getId, o -> BeanUtil.toBean(o, OrderSimpleResDTO.class)));
            return collect;
        }, OrderSimpleResDTO.class, ORDERS_PAGE_TTL);

        return orderSimpleResDTOS;

    }

//Old
//    @Override
//    public List<OrderSimpleResDTO> consumerQueryList(Long currentUserId, Integer ordersStatus, Long sortBy) {
//        //1.构件查询条件
//        LambdaQueryWrapper<Orders> queryWrapper = Wrappers.<Orders>lambdaQuery()
//                .eq(ObjectUtils.isNotNull(ordersStatus), Orders::getOrdersStatus, ordersStatus)
//                .lt(ObjectUtils.isNotNull(sortBy), Orders::getSortBy, sortBy)
//                .eq(Orders::getUserId, currentUserId)
//                .eq(Orders::getDisplay, EnableStatusEnum.ENABLE.getStatus())
//        Page<Orders> queryPage = new Page<>();
//        queryPage.addOrder(OrderItem.desc(SORT_BY));
//        queryPage.setSearchCount(false);
//
//        //2.查询订单列表
//        Page<Orders> ordersPage = baseMapper.selectPage(queryPage, queryWrapper);
//        List<Orders> records = ordersPage.getRecords();
//        List<OrderSimpleResDTO> orderSimpleResDTOS = BeanUtil.copyToList(records, OrderSimpleResDTO.class);
//        return orderSimpleResDTOS;
//
//    }
    /**
     * 根据订单id查询
     *
     * @param id 订单id
     * @return 订单详情
     */
    @Override
    public OrderResDTO getDetail(Long id) {
        //查询订单
//        Orders orders = queryById(id);
        //从快照表查询快照
        String currentSnapshotJson = orderStateMachine.getCurrentSnapshotCache(String.valueOf(id));
        OrderSnapshotDTO orderSnapshotDTO = JsonUtils.toBean(currentSnapshotJson, OrderSnapshotDTO.class);
        //懒加载方式取消支付超时的 订单
        orderSnapshotDTO = canalIfPayOvertime(orderSnapshotDTO);

        OrderResDTO orderResDTO = BeanUtil.toBean(orderSnapshotDTO, OrderResDTO.class);

        return orderResDTO;
    }
    /**
     * 如果支付过期则取消订单
     * @param orderSnapshotDTO
     */
    public OrderSnapshotDTO canalIfPayOvertime(OrderSnapshotDTO orderSnapshotDTO){
        //订单状态
        Integer ordersStatus = orderSnapshotDTO.getOrdersStatus();
        //判断订单是未支付且支付超时(从订单创建时间开始15分钟未支付)
//        if(ordersStatus==OrderStatusEnum.NO_PAY.getStatus() && orders.getCreateTime().isBefore(LocalDateTime.now().minusMinutes(15)) ){
        if(ordersStatus==OrderStatusEnum.NO_PAY.getStatus() && orderSnapshotDTO.getCreateTime().plusMinutes(15).isBefore(LocalDateTime.now())){
            //查询一下最新的支付状态，如果没有支付成功，再执行下边的取消代码
            OrdersPayResDTO payResultFromTradServer = ordersCreateService.getPayResultFromTradServer(orderSnapshotDTO.getId());
            //如果没有支付成功，再执行下边的取消代码
            if(ObjectUtils.isNotNull(payResultFromTradServer) && payResultFromTradServer.getPayStatus()!= OrderPayStatusEnum.PAY_SUCCESS.getStatus()){
                OrderCancelDTO orderCancelDTO = BeanUtils.toBean(orderSnapshotDTO,OrderCancelDTO.class);
                orderCancelDTO.setCurrentUserType(UserType.SYSTEM);
                orderCancelDTO.setCancelReason("订单支付超时系统自动取消");
                cancelByNoPay(orderCancelDTO);

                //从快照中查询订单数据
                String jsonResult = orderStateMachine.getCurrentSnapshotCache(String.valueOf(orderSnapshotDTO.getId()));
                orderSnapshotDTO = JSONUtil.toBean(jsonResult, OrderSnapshotDTO.class);
                return orderSnapshotDTO;
            }


        }

        return orderSnapshotDTO;

    }
    //old:
//    public Orders canalIfPayOvertime(Orders orders){
//        if(orders.getOrdersStatus() == OrderStatusEnum.NO_PAY.getStatus() && orders.getCreateTime().plusMinutes(15).isBefore(LocalDateTime.now())){
//            //查询最新支付状态，看是否是未支付
//            OrdersPayResDTO payResultFromTradServer = ordersCreateService.getPayResultFromTradServer(orders.getId());
//            if(ObjectUtils.isNotEmpty(payResultFromTradServer) && payResultFromTradServer.getPayStatus()!= OrderPayStatusEnum.PAY_SUCCESS.getStatus()){
//                //修改订单状态为取消
//                OrderCancelDTO orderCancelDTO = BeanUtil.toBean(orders, OrderCancelDTO.class);
//                orderCancelDTO.setCurrentUserType(UserType.SYSTEM);
//                orderCancelDTO.setCancelReason("订单超时支付，自动取消");
//                cancel(orderCancelDTO);
//                orders = getById(orders.getId());
//            }
//        }
//        return orders;
//    }



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
        //拷贝交易单号等信息到orderCancelDTO中
        orderCancelDTO.setRealPayAmount(orders.getRealPayAmount());
        orderCancelDTO.setTradingOrderNo(orders.getTradingOrderNo());
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
            //新起线程，保证即时退款
            ordersHandler.requestRefundNewThread(orderCancelDTO.getId());
        }else{
            throw new CommonException("当前订单状态不支持取消");
        }
    }

    /**
     * 管理端 - 分页查询
     *
     * @param orderPageQueryReqDTO 分页查询条件
     * @return 分页结果
     */
    @Override
    public PageResult<OrderSimpleResDTO> operationPageQuery(OrderPageQueryReqDTO orderPageQueryReqDTO) {
        //1.分页查询订单id列表
        Page<Long> ordersIdPage = operationPageQueryOrdersIdList(orderPageQueryReqDTO);
        if (ObjectUtil.isEmpty(ordersIdPage.getRecords())) {
            return PageUtils.toPage(ordersIdPage, OrderSimpleResDTO.class);
        }

        //2.根据订单id列表查询订单
        orderPageQueryReqDTO.setOrdersIdList(ordersIdPage.getRecords());
        List<Orders> ordersList = queryAndSortOrdersListByIds(orderPageQueryReqDTO);
        List<OrderSimpleResDTO> orderSimpleResDTOList = BeanUtil.copyToList(ordersList, OrderSimpleResDTO.class);

        //3.封装响应结果
        return PageResult.<OrderSimpleResDTO>builder()
                .total(ordersIdPage.getTotal())
                .pages(ordersIdPage.getPages())
                .list(orderSimpleResDTOList)
                .build();
    }

    /**
     * 管理端 - 分页查询订单id列表
     *
     * @param orderPageQueryReqDTO 分页查询模型
     * @return 分页结果
     */
    @Override
    public Page<Long> operationPageQueryOrdersIdList(OrderPageQueryReqDTO orderPageQueryReqDTO) {
        //1.构造查询条件
        Page<Orders> page = PageUtils.parsePageQuery(orderPageQueryReqDTO, Orders.class);
        LambdaQueryWrapper<Orders> queryWrapper = Wrappers.<Orders>lambdaQuery()
                .eq(ObjectUtil.isNotEmpty(orderPageQueryReqDTO.getContactsPhone()), Orders::getContactsPhone, orderPageQueryReqDTO.getContactsPhone())
                .eq(ObjectUtil.isNotEmpty(orderPageQueryReqDTO.getOrdersStatus()), Orders::getOrdersStatus, orderPageQueryReqDTO.getOrdersStatus())
                .eq(ObjectUtil.isNotEmpty(orderPageQueryReqDTO.getPayStatus()), Orders::getPayStatus, orderPageQueryReqDTO.getPayStatus())
                .eq(ObjectUtil.isNotEmpty(orderPageQueryReqDTO.getRefundStatus()), Orders::getRefundStatus, orderPageQueryReqDTO.getRefundStatus())
                .between(ObjectUtil.isAllNotEmpty(orderPageQueryReqDTO.getMinCreateTime(), orderPageQueryReqDTO.getMaxCreateTime()), Orders::getCreateTime, orderPageQueryReqDTO.getMinCreateTime(), orderPageQueryReqDTO.getMaxCreateTime())
                .gt(Orders::getUserId, 0)
                .select(Orders::getId);

        if (ObjectUtil.isNotEmpty(orderPageQueryReqDTO.getId())) {
            queryWrapper.eq(Orders::getId, orderPageQueryReqDTO.getId());
        } else {
            queryWrapper.gt(Orders::getId, 0);
        }

        //2.分页查询
        Page<Orders> ordersPage = baseMapper.selectPage(page, queryWrapper);

        //3.封装结果，查询数据为空，直接返回
        Page<Long> orderIdsPage = new Page<>();
        BeanUtil.copyProperties(ordersPage, orderIdsPage, "records");
        if (ObjectUtil.isEmpty(ordersPage.getRecords())) {
            return orderIdsPage;
        }

        //4.查询结果不为空，提取订单id封装
        List<Long> orderIdList = ordersPage.getRecords().stream().map(Orders::getId).collect(Collectors.toList());
        orderIdsPage.setRecords(orderIdList);
        return orderIdsPage;
    }


    /**
     * 根据订单id列表查询并排序
     *
     * @param orderPageQueryReqDTO 订单分页查询请求
     * @return 订单列表
     */
    @Override
    public List<Orders> queryAndSortOrdersListByIds(OrderPageQueryReqDTO orderPageQueryReqDTO) {
        //1.构造查询条件
        Page<Orders> page = new Page<>();
        page.setSize(orderPageQueryReqDTO.getPageSize());
        page.setOrders(PageUtils.getOrderItems(orderPageQueryReqDTO, Orders.class));
        LambdaQueryWrapper<Orders> queryWrapper = Wrappers.<Orders>lambdaQuery()
                .in(Orders::getId, orderPageQueryReqDTO.getOrdersIdList())
                .eq(ObjectUtils.isNotNull(orderPageQueryReqDTO.getUserId()), Orders::getUserId, orderPageQueryReqDTO.getUserId())
                .gt(ObjectUtils.isNull(orderPageQueryReqDTO.getUserId()), Orders::getUserId, 0);
        //2.分页查询
        page.setSearchCount(false);
        Page<Orders> ordersPage = baseMapper.selectPage(page, queryWrapper);
        if (ObjectUtil.isEmpty(ordersPage.getRecords())) {
            return Collections.emptyList();
        }
        return ordersPage.getRecords();
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
//        OrderUpdateStatusDTO orderUpdateStatusDTO = OrderUpdateStatusDTO.builder()
//                .id(orderCancelDTO.getId())
//                .originStatus(OrderStatusEnum.NO_PAY.getStatus())
//                .targetStatus(OrderStatusEnum.CANCELED.getStatus())
//                .build();
//        int result = ordersCommonService.updateStatus(orderUpdateStatusDTO);
//        if (result <= 0) {
//            throw new DbRuntimeException("订单取消事件处理失败");
//        }
        //引入订单状态状态机
        OrderSnapshotDTO orderSnapshotDTO=new OrderSnapshotDTO();
        orderSnapshotDTO.setCancelReason(orderCancelDTO.getCancelReason());
        orderSnapshotDTO.setCancelTime(LocalDateTime.now());
        orderSnapshotDTO.setCancellerId(orderCancelDTO.getCurrentUserId());
        orderStateMachine.changeStatus(orderCancelDTO.getId().toString(), OrderStatusChangeEventEnum.CANCEL, orderSnapshotDTO);
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
//        //update orders set orders_status = 700 where id = ? and orders_status = 100;
//        OrderUpdateStatusDTO orderUpdateStatusDTO = OrderUpdateStatusDTO.builder()
//                .id(orderCancelDTO.getId())
//                .originStatus(OrderStatusEnum.DISPATCHING.getStatus())
//                .targetStatus(OrderStatusEnum.CLOSED.getStatus())
//                .refundStatus(OrderRefundStatusEnum.REFUNDING.getStatus())
//                .build();
////        orderUpdateStatusDTO.setRefundStatus(OrderRefundStatusEnum.REFUNDING.getStatus());
//        int result = ordersCommonService.updateStatus(orderUpdateStatusDTO);
//        if (result <= 0) {
//            throw new DbRuntimeException("订单取消事件处理失败");
//        }
//        //保存退款记录
//        OrdersRefund ordersRefund= BeanUtils.toBean(orderCancelDTO,OrdersRefund.class);
//        boolean save = ordersRefundService.save(ordersRefund);
//        if (!save){
//            log.info("保存派单中订单退款记录失败！");
//        }
        OrderSnapshotDTO orderSnapshotDTO=new OrderSnapshotDTO();
        orderSnapshotDTO.setCancelReason(orderCancelDTO.getCancelReason());
        orderSnapshotDTO.setCancelTime(LocalDateTime.now());
        orderSnapshotDTO.setCancellerId(orderCancelDTO.getCurrentUserId());
        orderStateMachine.changeStatus(orderCancelDTO.getId().toString(), OrderStatusChangeEventEnum.CLOSE_DISPATCHING_ORDER, orderSnapshotDTO);
    }
}
