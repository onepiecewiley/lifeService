package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_ID_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result queryTypeList() {
        // 1.首先去redis里面查询
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> shopTypes = redisTemplate.opsForList().range(CACHE_SHOP_ID_KEY, 0, -1);

        // 2.如果查询到了，直接返回
        if(!shopTypes.isEmpty()){
            // 反序列化
            return Result.ok(shopTypes.stream()
                    .map(shopTypeJson -> {
                        try {
                            return objectMapper.readValue(shopTypeJson, ShopType.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList()));
        }

        // 3.如果查询不到，再去数据库查询，将查询到的数据存入redis，并返回
        List<ShopType> result = query().list();
        if(result.isEmpty()){
            return Result.fail("没有查询到店铺类型");
        }

        //序列化每个对象  序列化成String类型的List 然后保存
        List<String> collect = result.stream().map(shopType -> {
            try {
                return objectMapper.writeValueAsString(shopType);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
                }
        ).collect(Collectors.toList());
        //写入 redis
        redisTemplate.opsForList().rightPushAll(CACHE_SHOP_ID_KEY,collect); //必须是String类型的
        return Result.ok(result);
    }
}
