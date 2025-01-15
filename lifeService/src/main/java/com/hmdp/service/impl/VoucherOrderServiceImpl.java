package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StreamOperations;
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
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;
    //private BlockingQueue<VoucherOrder> blockingQueue = new ArrayBlockingQueue<>(1000);
    private ExecutorService executorService = Executors.newFixedThreadPool(30);
    private IVoucherOrderService proxy;   // 获取代理对象 要熟悉整个bean的加载流程才行

    @PostConstruct   // 初始化之前执行
    private void init() {
        // proxy的初始化不能放在这里 否则会出问题 AOP代理对象的生成是在 postConstruct方法后做的
        // AOP的增强是在 初始化之后做的
        System.out.println("开始执行后代线程....");
        executorService.submit(new VoucherOrderHandler());
    }
   /* class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            // run方法里面要不断去查询阻塞队列判断是否有任务进来 有的话就执行
            while (true){
                VoucherOrder task = null;
                try {
                    task = blockingQueue.take();
                    handleVoucherOrder(task);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }*/

    private String queueStream = "stream.orders";
    private void handlePendingList() {
        while (true) {
            VoucherOrder task = null;
            try {
                // 1. 先去消息队列里面取消息 取最新的消息  String是消息的id  其他是消息中的 键值对
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueStream, ReadOffset.from("0"))
                );
                // 2. 如果消息是空的 说明Pending队列里面现在没有积压的订单 直接break 跳出while 外面调用的函数再去取还未被处理的消息
                if (list == null || list.isEmpty()) {
                    break;
                }
                // 3. 判断一下消息是否为空 如果不空 则可以进行处理 写入数据库
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> order = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(order, new VoucherOrder(), true);
                handleVoucherOrder(voucherOrder);
                // 4. 要对处理过的消息进行确认
                stringRedisTemplate.opsForStream().acknowledge(queueStream, "g1", record.getId());
            } catch (InterruptedException e) {
                // 异常了 继续去处理
                log.error("处理订单异常", e);
            }
        }

    }


    class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            // run方法里面要不断去查询阻塞队列判断是否有任务进来 有的话就执行
            System.out.println("后台线程函数开始执行了...");
            while (true) {
                try {
                    // 1. 先去消息队列里面取消息 取最新的消息  String是消息的id  其他是消息中的 键值对
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueStream, ReadOffset.lastConsumed())
                    );
                    // 2. 如果消息是空的 说明消息队列里面现在没有积压的订单 直接continue 进行下一次循环重复获取
                    if (list == null || list.isEmpty()) {
//                        System.out.println("list:" + list);
                        continue;
                    }
                    System.out.println("不空 插入数据库");
                    // 3. 判断一下消息是否为空 如果不空 则可以进行处理 写入数据库
                    MapRecord<String, Object, Object> record = list.get(0);
                    System.out.println("record: " + record);
                    Map<Object, Object> order = record.getValue();
                    System.out.println("order: " + order);
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(order, new VoucherOrder(), true);
                    System.out.println("voucherOrder: " + voucherOrder);
                    handleVoucherOrder(voucherOrder);
                    // 4. 要对处理过的消息进行确认
                    System.out.println("执行消息确认...");
                    stringRedisTemplate.opsForStream().acknowledge(queueStream, "g1", record.getId());
                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                    // 发生异常会导致订单积压
                    handlePendingList();
                }
            }
        }
    }


    @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        // 执行lua脚本 先查看库存是否存在是否足够 库存足够了 再去判断用户是否下过单了
        DefaultRedisScript defaultRedisScript = new DefaultRedisScript();
        defaultRedisScript.setLocation((org.springframework.core.io.Resource) new ClassPathResource("seckill.lua"));
        defaultRedisScript.setResultType(Long.class);

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long res = (Long) stringRedisTemplate.execute(defaultRedisScript, Collections.emptyList(), voucherId.toString(), userId.toString(), String.valueOf(orderId));

        if (res == 1) {
            //库存不足
            return Result.fail("库存不足");
        }

        if (res == 2) {
            //用户已经购买过一次
            return Result.fail("请勿重复购买");
        }

        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);

    }

/*    @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        
        // 2.判断秒杀是否开始
        LocalDateTime beginTime = voucher.getBeginTime();
        if(beginTime.isAfter(LocalDateTime.now())){
            return Result.fail("秒杀活动未开始");
        }
        
        // 3.判断秒杀是否结束
        LocalDateTime endTime = voucher.getEndTime();
        if(endTime.isBefore(LocalDateTime.now())){
            return Result.fail("秒杀活动已结束");
        }

        // 4.判断库存是否充足
        Integer stock = voucher.getStock();
        if(stock < 1) {
            return Result.fail("库存不足");
        }

        // 补充: 这里要实现一人一单的业务
        // 通过用户id和优惠券的id查询订单是否存在 如果存在则直接返回异常 提示已经买过了 否则再扣减库存
        // 在插入新的订单数据的时候 不能一次性插入多条 为了防止同一个用户短时间内多次购买 在进行数据库账单创建的时候 需要先去获取锁  创建完释放锁
        // 之后如果订单已经存在了 再去获取锁 然后创建订单的话 会通过创建订单的函数去做判断

        Long userId = UserHolder.getUser().getId();

        RLock lock = redissonClient.getLock("lock:order" + userId); // 锁的名称

        // 获取锁
        boolean isLock = lock.tryLock(1,1200, TimeUnit.SECONDS); //最长等待时间 锁的持有时间 单位

        if(!isLock){
            // 获取锁失败
            return Result.fail("不允许重复下单");
        }

        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }finally {
            lock.unlock();
        }

*//*
        仅仅只能够保证单体项目的前提下 能够实现一人一单并且线程安全
        如果同一个用户同时发送了多个请求 在分布式项目中这些请求有可能进入多个不同的服务中 从而无法保证一人一单
        //保证锁的粒度细致到用户的身上 intern() 方法会检查字符串常量池中是否已经有相同内容的字符串：如果有，返回常量池中的那个字符串的引用。如果没有，把当前字符串添加到常量池，并返回该字符串的引用。


        synchronized (userId.toString().intern()) {  //加在这里的目的是为了防止spring提交事务的时候导致的并发问题
            // 获取代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }*//*
    }*/

   /* @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        // 执行lua脚本 先查看库存是否存在是否足够 库存足够了 再去判断用户是否下过单了
        DefaultRedisScript defaultRedisScript = new DefaultRedisScript();
        defaultRedisScript.setLocation((org.springframework.core.io.Resource) new ClassPathResource("seckill.lua"));
        defaultRedisScript.setResultType(Long.class);

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long res = (Long)stringRedisTemplate.execute(defaultRedisScript, Collections.emptyList(), voucherId.toString(), UserHolder.getUser().toString(), String.valueOf(orderId));

        if(res == 1){
            //库存不足
            return Result.fail("库存不足");
        }

        if(res == 2){
            //用户已经购买过一次
            System.out.println(res);
            return Result.fail("请勿重复购买");
        }

        // 返回值是0 则说明可以下单 这个时候返回订单编号 然后将任务塞给阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        blockingQueue.add(voucherOrder);

        proxy = (IVoucherOrderService)AopContext.currentProxy();
        return Result.ok(orderId);

    }*/
/*    @Transactional    //由spring去提交事务
      public Result createVoucherOrder(Long voucherId) {
            Long userId = UserHolder.getUser().getId();
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if(count > 0) {
                return Result.fail("用户已经购买过一次！不能重复购买");
            }

            // 5.扣减库存  这里有并发安全问题
            boolean isSuccess = seckillVoucherService.update().setSql("stock = stock - 1")
                    .gt("stock",0) //版本思想... 防止并发问题
                    .eq("voucher_id", voucherId).update();

            if(!isSuccess) {
                return Result.fail("库存不足");
            }

            //6.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 6.1.订单id
            long orderId = redisIdWorker.nextId("order"); //订单id缓存了
            voucherOrder.setId(orderId);
            // 6.2.用户id
            voucherOrder.setUserId(userId);
            // 6.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            System.out.println("执行结束........");
            return Result.ok(orderId);
    }*/

    public void handleVoucherOrder(VoucherOrder order) throws InterruptedException {
        Long userId = order.getId();


        RLock lock = redissonClient.getLock("lock:order" + userId); // 锁的名称

        // 获取锁
        boolean isLock = lock.tryLock(1, 1200, TimeUnit.SECONDS); //最长等待时间 锁的持有时间 单位

        if (!isLock) {
            // 获取锁失败
            log.error("不允许重复下单");
            return;
        }

        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy(); // 没用了这里 如果这么写 因为这是基于ThreadLocal获取
            System.out.println("执行到这里...");
            proxy.createVoucherOrder(order);
        } finally {
            lock.unlock();
        }
        return;
    }

    @Transactional    //由spring去提交事务
    public void createVoucherOrder(VoucherOrder order) {
        Long userId = order.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", order.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次！不能重复购买");
            return;
        }

        // 5.扣减库存  这里有并发安全问题
        boolean isSuccess = seckillVoucherService.update().setSql("stock = stock - 1")
                .gt("stock", 0) //版本思想... 防止并发问题
                .eq("voucher_id", order.getVoucherId()).update();

        if (!isSuccess) {
            log.error("库存不足");
            return;
        }

        //6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 6.1.订单id
        long orderId = redisIdWorker.nextId("order"); //订单id缓存了
        voucherOrder.setId(orderId);
        // 6.2.用户id
        voucherOrder.setUserId(userId);
        // 6.3.代金券id
        voucherOrder.setVoucherId(orderId);
        System.out.println("voucherOrder: " + voucherOrder);
        save(voucherOrder);
        System.out.println("执行结束........");
        return;
    }
}
