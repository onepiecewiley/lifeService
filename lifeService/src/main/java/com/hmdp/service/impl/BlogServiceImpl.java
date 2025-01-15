package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.ScrollResult;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    public IUserService userService;

    @Resource
    public StringRedisTemplate stringRedisTemplate;

    @Resource
    public IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        //  查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return null;
        }
        // 查询blog作者
        QueryBlogUser(blog);
        // 查询用户是否点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    @Override
    public List<Blog> queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据 仅仅获取还不够 还要得到哪些用户点赞了这个blog
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            QueryBlogUser(blog);
            isBlogLiked(blog);
        });
        return records;
    }

    @Override
    public Result likeBlog(Long id) {
        // 首先去redis里面查看 是否有当前用户对当前blog 的点赞
        String key = BLOG_LIKED_KEY + id.toString();
        Long userId = UserHolder.getUser().getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if(score == null){
            //没点赞 修改点赞数量 同时添加完毕后要写入redis中
            //修改点赞的数量
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else{
            // 点过赞了 再点赞就是取消
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if(isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1.查询top5的点赞用户 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 2.解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 3.根据用户id查询用户 WHERE id IN ( 5 , 1 ) ORDER BY FIELD(id, 5, 1)
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 4.返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result queryBlogByUserId(Long id, Integer current) {
        Page<Blog> page = query().eq("user_id",id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取到当页的数据后 就可以返回
        List<Blog> res = page.getRecords();
        System.out.println(res);
        return Result.ok(res);
    }

    @Override
    public Result queryBlogOfFollow(Long lastId, Integer offset) {
        // 实现滚动分页查询
        /*
        * 1. 根据自己的用户id 去redis中查找到对应的集合 找到自己要读取的blog的id
        * 2. 根据lastId 和 offset 确定要查询的blog对应的id
        * 3. 根据id查询blog
        * */
        Long id = UserHolder.getUser().getId();
        String key = FEED_KEY + id;
        // Set<TypedTuple<V>> reverseRangeByScoreWithScores(K var1, double var2, double var4, long var6, long var8);
        Set<ZSetOperations.TypedTuple<String>> typedTuples =
                stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, lastId, offset, 2);
        System.out.println("now 。。。。。");
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok(); // 返回空对象
        }
        System.out.println("not now......");
        // 不空 这个时候得到的这个set 里面放的都是我们需要的blog的 id  我们首先得通过blog的id去获取每一个blog
        // 并且 我们需要给前端返回上次查询的时间戳的下界 和 它的数量offset 因为下次查询不需要再用到这个时间戳对应的数据
        List<Blog> blogs = new ArrayList<>(typedTuples.size()); //这里需要指定长度 否则默认长度是16 可能小于集合的长度
        //由于需要返回最小的时间戳 得记录最小时间戳和其个数
        double minTime = 0;
        int os = 1;
        for(ZSetOperations.TypedTuple<String> typedTuple : typedTuples){
            String blogId = typedTuple.getValue(); // 获取每个blog的id
            Double score = typedTuple.getScore(); // 获取到每个blog对应的score
            //根据blogId查询对应的blog
            Blog blog = getById(Long.valueOf(blogId)); //效率不高 每次都在查数据库 可以先把id筛出来 放到list 然后通过List查数据库 一次就好了 暂时就这样吧
            QueryBlogUser(blog);
            isBlogLiked(blog);
            blogs.add(blog);

            if(score != minTime){
                minTime = score;
                os = 1;
            }else {
                os++;
            }
        }
        //os = minTime == lastId ? os : os + offset;

        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime((long) minTime);
        scrollResult.setOffset(os);

        return Result.ok(scrollResult);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增失败");
        }
        // 同时为了后边的推送 即 一个博主要把自己的内容推送给自己的粉丝 粉丝要有一个接收缓冲区 这样在接收的时候不需要太高的延迟
        // 三个方法 1. 拉  2. 推  3. 推拉    这里只需要实现推 推给自己的每一个粉丝
        // 查询自己的粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        System.out.println(follows);

        for(Follow f : follows){
            Long userId = f.getUserId(); //得到每个粉丝的id
            String key = FEED_KEY + userId;
            // 将blog的 id写入每个key键所对应的redis 集合
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis()); //时间戳当作score
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    private void isBlogLiked(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        String key = "blog:liked:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    private void QueryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

}
