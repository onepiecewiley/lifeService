package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

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
public class LoginInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //前面该保存到ThreadLocal 这些事情 都干过了 现在要干的就是放行还是不放行
        //查看ThreadLocal中有没有这个对象 没有就不放行（因为如果合法 上一个拦截器就已经把对象放到ThreadLocal里面了)
        if (UserHolder.getUser() == null) {
            System.out.println("错了");
            response.setStatus(401);
            return false;
        }
        return true;
    }
}