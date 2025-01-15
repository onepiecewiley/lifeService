package com.hmdp.config;

import com.hmdp.interceptor.LoginInterceptor;
import com.hmdp.interceptor.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

/**
 * @author onepiecewiley
 * @version 1.0
 * @date 2024/11/11 16:06
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Resource
    private RefreshTokenInterceptor refreshTokenInterceptor;

    @Resource
    private LoginInterceptor loginInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(refreshTokenInterceptor).order(0).addPathPatterns("/**"); //数字小优先级更高  注意这里的拦截器都是new出来的
        registry.addInterceptor(loginInterceptor)
                .excludePathPatterns("/user/code","/user/login","/shop/**","/shop-type/**","/upload/**","/blog/hot","/blog/search","/voucher/**","/voucher-order/**").order(1);
    }
}
