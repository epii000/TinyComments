package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.redis.RedisConstants.BLOG_LIKED_KEY;

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
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.isBlogLiked(blog);
            this.queryBlogUser(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        // 1、获取博客
        Blog blog = getById(id);
        // 2、判断博客是否为空
        if (blog == null) {
            // 为空，表示博客不存在，返回错误信息
            return Result.fail("这篇博客不存在");
        }
        queryBlogUser(blog);
        // 查询blog是否被点赞过
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 1、获取用户
        UserDTO user = UserHolder.getUser();
        // 1.1、判断用户是否存在
        if (user == null) {
            // 用户未登录，无需查询是都点赞
            return;
        }
        // 1.2、获取用户id
        Long userId = user.getId();
        // 2、获取用户在redis中点赞博客信息
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // 3、设置博客的点赞字段
        blog.setIsLike(score != null);
    }

    @Override
    @Transactional
    public Result likeBlog(Long id) {
        // 1、获取用户id
        Long userId = UserHolder.getUser().getId();
        // 2、判断用户是否点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 3、如果未点赞
            try {
                // 3.1、数据库该用户点赞数加一
                boolean isIncreaseSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
                // 3.2、redis 中存储用户点赞数据（sortedSet 集合）
                if (isIncreaseSuccess) {
                    stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
                }
            } catch (Exception e) {
                    // 出现异常进行回滚
                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                    return Result.fail("点赞失败");
                }
        } else {
            /// 4、如果已经点赞
            try {
                // 4.1、数据库该用户点赞数减一
                boolean isDecreaseSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
                // 4.2、redis 中删除用户点赞数据
                if (isDecreaseSuccess) {
                    stringRedisTemplate.opsForZSet().remove(key, userId.toString());
                }
            } catch (Exception e) {
                // 出现异常进行回滚
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                return Result.fail("取消点赞失败");
            }
        }
        return Result.ok("点赞或取消点赞成功");
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 1、查询redis中点赞前五名的用户 zrange key 0 4
        String key = BLOG_LIKED_KEY + id;
        Set<String> range = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        // 2、判断是否有用户点赞
        if (range == null || range.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 3、有用户点赞，则从redis存储的博客点赞集合中解析用户id
        List<Long> userIds = range.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", userIds);
        // 4、根据用户id获得用户，封装成UserDto对象返回（排除用户敏感信息）
        // 【直接用userService.listByIds的结果点赞顺序是反的，需要使用MybatisPlus提供的自定义查询】
        // WHERE id IN (5, 1) ORDER BY FIELD (id, 5, 1)
        List<UserDTO> userDTOS = userService.query()
                .in("id", userIds).last("ORDER BY FIELD (id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 5、返回
        return Result.ok(userDTOS);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
