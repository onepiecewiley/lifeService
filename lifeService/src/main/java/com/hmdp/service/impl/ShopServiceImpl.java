package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) throws InterruptedException {
        // 逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(id,CACHE_SHOP_KEY, Shop.class, (idd) -> getById(idd));
        if (shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        //先更新库 在去删除缓存 用来维护一致性  至于为什么这么做 出错概率小一点
        updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Shop queryWithMutex(Long id) throws InterruptedException {
        // 解决缓存击穿
        /*
         * 为了解决缓存穿透问题 现在如果查不到这个值 先去redis中查看这个键是否存在 存在则返回 如果不存在去数据库查询 如果存在写入redis缓存 如果不存在也写入redis缓存
         * 不过这个时候写入的是一个空值
         * */
        // 1.查redis 看商铺是否存在
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if(StrUtil.isNotBlank(shopJson)){ //判断非空且非空白
            // 2.存在，直接返回
            return  JSONUtil.toBean(shopJson, Shop.class); // 将其转换成Shop类型的对象
        }
        // 判断空不空
        if(shopJson != null){ //不空 但是空白 那说明根本不存在 redis里存的是一个空对象 防止缓存穿透的
            return null;
        }

        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try{
            // 4. 为了防止大量请求来到数据库 这里得做一下处理
            // 首先获取互斥锁 保证对数据库操作是互斥的
            boolean isLock = tryLock(lockKey);

            if(!isLock){
                // 失败 则休眠
                Thread.sleep(50);
                //递归调用 再去判断  这里必须return  不return  即使在递归的时候获取到了数据 返回递归的第一层之后 也会继续往下执行 请求会达到数据库
                return queryWithMutex(id);  // !!!!!!!
            }

            // 成功 再去查询
            shop = getById(id);
            System.out.println("请求到达数据库");

            //模拟重建延时
            Thread.sleep(200);

            //数据库里面也不存在 将它写入redis缓存 写入空值
            if(shop == null){
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_NULL_KEY,JSONUtil.toJsonStr(new Shop()),2L,TimeUnit.MINUTES);
                return null;
            }
            // 5.数据库存在，存入redis，并返回
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        }catch(RuntimeException e){
            throw new RuntimeException(e);
        }finally {
            // 释放锁
            unlock(lockKey);
        }
        return shop;
    }

    @Override
    public Shop QueryWithThrough(Long id) {
        /*
         * 为了解决缓存穿透问题 现在如果查不到这个值 先去redis中查看这个键是否存在 存在则返回 如果不存在去数据库查询 如果存在写入redis缓存 如果不存在也写入redis缓存
         * 不过这个时候写入的是一个空值
         * */
        // 1.查redis 看商铺是否存在
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if(StrUtil.isNotBlank(shopJson)){ //判断非空且非空白
            // 2.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class); // 将其转换成Shop类型的对象
        }
        // 判断空不空
        if(shopJson != null){ //不空 但是空白 那说明根本不存在 redis里存的是一个空对象 防止缓存穿透的
            return null;
        }
        // 3.不存在，查数据库
        Shop shop = getById(id);

        //数据库里面也不存在 将它写入redis缓存 写入空值  这里会不会有并发问题？？？ 多个线程都没有找到缓存 都去查数据库 如果数据库还是没有 都将其写入redis会不会重复写入同一个key的空值？？？
        // 并且服务器压力会不会太大了
        if(shop == null){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_NULL_KEY,JSONUtil.toJsonStr(new Shop()),2L,TimeUnit.MINUTES);
            return null;
        }

        // 5.数据库存在，存入redis，并返回
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    @Override
    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //空的 则返回null 空的说明不在缓存里面 也不在数据库里面 就是没有  如果是热点数据 一定是在redis里面的 查不到说明不是热点
            return null;
        }
        
        // 命中 现在去查看过期了没 查看过期时间 首先得转一下 将其转换成一个RedisData类型的数据
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);

        if(expireTime.isAfter(LocalDateTime.now())){ //如果过期时间在当前时间之后 那就没有过期 直接返回即可
            return shop; //必须反序列化
        }

        // 过期了需要重建缓存 即从数据库中读取最新的结果 然后更新缓存 这个过程得创建一个新的线程来完成
        // 首先获取锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if(!isLock){
            // 获取锁失败 直接返回旧的数据
            //System.out.println("已经过期了，没获取到锁，直接返回");
            return shop;
        }

        //获取锁成功 开启一个新的线程 让它去更新缓存 然后更新完让它释放锁
        //System.out.println("已经过期了，获取到锁");
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            try{
                saveShop2Redis(id,20L);
            }catch (Exception e){
                throw new RuntimeException(e);
            }finally {
                unlock(lockKey);
            }
        });

        return shop;
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        String key = SHOP_GEO_KEY + typeId;

        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页 因为geo查询 只能查找0 - n 条 下限没办法自定义
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 查找redis 用radius查
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().radius(
                key,
                new Circle(new Point(x, y),
                        new Distance(5,Metrics.KILOMETERS)
                ),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                        .includeCoordinates().sortAscending().limit(end));

        // 查出来的都是店铺的指定半径范围内店铺的id 要根据id查询对应的店铺信息
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();

        if(content.size() <= from){
            return Result.ok(Collections.emptyList());
        }

        // 得到所有商品地理位置信息的封装对象列表后  现在需要获取每个对象的id 方便后面的查询
        List<Long> ids = new ArrayList<>(content.size());
        Map<String,Distance> distanceMap = new HashMap<>(content.size());
        // GeoResult<RedisGeoCommands.GeoLocation<String>> 也就是说这个东西包括了 地理位置信息（经纬度，名字） 还有距离中心的距离
        content.stream().skip(from).forEach(result -> {
           // 获取店铺id
           String shopIdStr = result.getContent().getName(); // 获取地理位置信息 中的 名字
           ids.add(Long.valueOf(shopIdStr));
           // 获取店铺距离 距离中心点的距离
           Distance distance = result.getDistance();  // 获取距离中心的距离
           distanceMap.put(shopIdStr,distance);
        });

        // 根据id查询店铺信息
        String join = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", join).last("ORDER BY FIELD(id," + join + ")").list();
        for (Shop shop : shops) {
            // 获取店铺距离
            String shopIdStr = shop.getId().toString();
            Distance distance = distanceMap.get(shopIdStr);
            // 将距离封装到店铺对象中
            shop.setDistance(distance.getValue());
        }

        return Result.ok(shops);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);  //直接返回flag 可能导致拆箱的时候空指针异常
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id,Long expireSeconds){
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 2.写入redis
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
