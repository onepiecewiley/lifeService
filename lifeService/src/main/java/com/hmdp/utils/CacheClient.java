package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author onepiecewiley
 * @version 1.0
 * @date 2024/11/14 14:17
 * 基于StringRedisTemplate封装一个缓存工具类，满足下列需求：
 *
 * * 方法1：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
 * * 方法2：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
 *
 * * 方法3：根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
 * * 方法4：根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
 */
@Component
@Slf4j
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;


    // 方法一
    public void set(String key, String value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, value, time, unit);
    }

    // 方法二 解决缓存击穿的问题
    public void setWithLogicalExpire(String key, String value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //方法三 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题 反序列化需要指定具体的类型
    public <T,ID> T queryWithPassThrough(ID id, String searchKey, Class<T> clz, Function<ID,T> dbFallBack){
        String key = searchKey + id;

        // 1.从redis中查询数据
        String jsonStr = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(jsonStr)) { // 非空且非空值 ""
            // 3.1 存在，直接返回
            return JSONUtil.toBean(jsonStr, clz);
        }

        if(jsonStr != null){
            return null;
        }

        // 3.2 不存在，根据id查询数据库
        T r = dbFallBack.apply(id);
        if(r == null){
            // 不存在 存空值到redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        // 3.3 存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),30,TimeUnit.MINUTES);
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <T,ID> T queryWithLogicalExpire(ID id, String searchKey, Class<T> clz, Function<ID,T> dbFallBack) {
        String key = searchKey + id;
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
        T res = JSONUtil.toBean(data, clz);

        if(expireTime.isAfter(LocalDateTime.now())){ //如果过期时间在当前时间之后 那就没有过期 直接返回即可
            return res; //必须反序列化
        }

        // 过期了需要重建缓存 即从数据库中读取最新的结果 然后更新缓存 这个过程得创建一个新的线程来完成
        // 首先获取锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if(!isLock){
            // 获取锁失败 直接返回旧的数据
            //System.out.println("已经过期了，没获取到锁，直接返回");
            return res;
        }

        //获取锁成功 开启一个新的线程 让它去更新缓存 然后更新完让它释放锁
        //System.out.println("已经过期了，获取到锁");
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            try{
                saveShop2Redis(searchKey,id,20L, dbFallBack);
            }catch (Exception e){
                throw new RuntimeException(e);
            }finally {
                unlock(lockKey);
            }
        });

        return res;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);  //直接返回flag 可能导致拆箱的时候空指针异常
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public <ID,T> void saveShop2Redis(String seatchKey,ID id,Long expireSeconds,Function<ID,T> dbFallBack){
        // 1.查询店铺数据
        T res = dbFallBack.apply(id);
        // 2.写入redis
        RedisData redisData = new RedisData();
        redisData.setData(res);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(seatchKey + id, JSONUtil.toJsonStr(redisData));
    }
}
