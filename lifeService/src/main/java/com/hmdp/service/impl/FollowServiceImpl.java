package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public Result isFollow(Long followUserId) {
        // 判断是否关注了这个用户 先获取当前用户的id 然后去通过当前用户id和要关注的用户id查找表 返回数量
        Long id = UserHolder.getUser().getId();

        Integer count = query().eq("user_id", id).eq("follow_user_id", followUserId).count();

        return Result.ok(count > 0);
    }

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 由于查找共同关注需要依赖redis的set集合结构 所以现在 只要是进行了新增关注 都要新增的信息写入redis的set结构中方便下游业务的使用
        // 首先获取当前用户id
        Long id = UserHolder.getUser().getId();
        String key = "follows:" + id; // 当前用户关注了什么人
        if(isFollow){
            // 如果是关注 则向数据库插入数据

            Follow follow = new Follow();
            follow.setUserId(id);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if(isSuccess) {
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }else{
            // 否则是取消关注
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", id).eq("follow_user_id", followUserId));
            if(isSuccess){
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result followCommons(Long userId) {
        System.out.println("执行到这里了.....");
        // 查询当前用户的id 和 传入的userId 的共同关注列表
        Long id = UserHolder.getUser().getId();//当前用户的id
        String key1 = "follows:" + id; // 当前用户关注了什么人
        String key2 = "follows:" + userId; // 传入的userId关注了什么人
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2); // 这里面全都是id

        if(intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }

        //得到共同关注的id 进行解析
        List<Long> intersectId = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询这些用户并返回
        List<UserDTO> users = userService.listByIds(intersectId).stream().map(
                user -> BeanUtil.copyProperties(user, UserDTO.class)
        ).collect(Collectors.toList());

        return Result.ok(users);

    }
}
