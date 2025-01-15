package com.hmdp.interceptor;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * @author onepiecewiley
 * @version 1.0
 * @date 2024/11/11 15:58
 */

@Component
@Slf4j
public class RefreshTokenInterceptor implements HandlerInterceptor {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

//    public RefreshTokenInterceptor(RedisTemplate redisTemplate) {
//        this.redisTemplate = redisTemplate;
//    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String token = request.getHeader("authorization");
        //System.out.println("token: " + token);
        if(StrUtil.isBlank(token)){
            return true;
        }
        String key = LOGIN_USER_KEY + token;
        //System.out.println("key: " + key);
        Map<Object,Object> userMap = stringRedisTemplate.opsForHash().entries(key);
        //System.out.println("userMap: " + userMap);

        if(userMap.isEmpty()){ //放行 刚开始没登陆就是空的 必须放行 后面如果空的也不用担心 如果是空的ThreadLocal肯定没东西 如果token不空 这个是空的 只能说明token过期了
            return true;
        }

        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        // 用于将 Map 中的键值对填充到 Java Bean（如 UserDTO）的属性中 false：表示是否忽略 null 值。如果设为 false，userMap 中 null 值会覆盖 UserDTO 中的属性；设为 true 时则忽略 null 值。
        UserHolder.saveUser(userDTO);
        //刷新token的有效期
        stringRedisTemplate.expire(key,LOGIN_USER_TTL, TimeUnit.MINUTES);  //再次登录只要这个token还在 就要刷新这个token的有效期
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
