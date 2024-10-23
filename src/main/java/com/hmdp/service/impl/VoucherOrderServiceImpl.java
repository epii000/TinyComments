package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.redis.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    public static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 静态代码块初始化脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // 设置位置
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        // 设置结果类型
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 创建线程池(一个线程)
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 类初始化完毕后执行
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
    }

    private class VoucherOrderHandle implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                // 1、获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2、判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 2.1、获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 3、获取消息成功，解析消息中的订单信息，转换为VoucherOrder对象
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4、创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5、进行ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常" + e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1、获取pengding-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2、判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 2.1、获取失败，说明pengding-list中没有消息，结束循环
                        break;
                    }
                    // 3、获取消息成功，解析消息中的订单信息，转换为VoucherOrder对象
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4、创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5、进行ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pengding-list订单异常" + e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }


/*    // 阻塞队列
    // 特点：线程尝试从队列中获取元素时，没有元素就会被阻塞，直到队列中有元素才会被唤醒获取元素
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    // 线程任务(秒杀抢购前执行 --> 类初始化时就执行)
    private class VoucherOrderHandle implements Runnable{

        @Override
        public void run() {
            while (true) {
                try {
                    // 1、获取队列中的订单信息
                    // take:必要时等待，直到元素可用为止。没有元素不会往下执行,不会造成cpu负担
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2、创建订单
                    handleVoucherOrder(voucherOrder);

                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        proxy.createVoucherOrder(voucherOrder);
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 获取订单id
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.判断结构是否为零
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不可重复购买");
        }
        // 3、获取代理对象（只能提前获取，开启了新线程，在子线程中无法获取）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4、返回订单id
        return Result.ok(orderId);
    }

        /**
     * 优惠券秒杀下单(Lua代码加上阻塞队列实现)
     * 基于阻塞队列实现异步秒杀（主线程判断秒杀资格完成抢单业务，而耗时久的业务放入阻塞队列，利用独立线程异步执行）
     * 存在问题：
     * 1、使用jdk中的阻塞队列，防止高并发导致内存溢出，限制了阻塞队列长度（内存限制问题）
     * 2、基于内存保存订单信息，如果服务宕机，内存中的所有订单信息都会丢失，用户付款但是后台没数据，导致数据不一致。
     * 还可能有一个线程从队列中取出任务正在执行，此时发生事故，导致任务未执行，任务取消会导致队列消失，以后不再执行任务，从而也会出现数据不一致
     *
     * @return
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 1、执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(), // 传入空集合
                voucherId.toString(), userId.toString()
        );
        // 2、判断脚本结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1、结果不为0，表示没有购买资格（库存不足或已经下过单）
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2、结果为0，有购买资格，把下单信息存到阻塞队列中，用于异步执行
        // 创建voucherOrder对象
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3、设置订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4、设置用户id
        voucherOrder.setUserId(userId);
        // 2.5、设置代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6、创建阻塞队列
        orderTakes.add(voucherOrder);

        // 3、获取代理对象（只能提前获取，开启了新线程，在子线程中无法获取）
        proxy = (VoucherOrderService) AopContext.currentProxy();
        // 4、返回订单id
        return Result.ok(orderId);
    }*/

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始！");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
        // intern()函数，只要String的值一样，地址就是一致的
//        synchronized (userId.toString().intern()) {
//            // Spring 事务失效的几种可能性？
//            // 获取事务代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }
//        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            // 获取事务代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单
        Long userId = voucherOrder.getUserId();
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();
        if (count > 0) {
            log.error("一人只能购买一次");
            return;
        }
        // 乐观锁实现多线程访问，扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        /*(未使用阻塞队列改变前)
        // 6、生成券订单对象，创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 6.1、创建订单id(用自定义id生成器随机生成)
        long orderId = redisIdWorker.nextId("order:");
        voucherOrder.setId(orderId);

        // 6.2、设置用户id
        voucherOrder.setUserId(userId);

        // 6，3、代金券id
        voucherOrder.setVoucherId(voucherId);*/

        // 6、保存订单
        save(voucherOrder);

        /*// 7、返回订单id
        return Result.ok(orderId);*/
    }
}
