package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.redis.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // todo 只是使用log记录验证码，并未真实调用第三方平台实现
        log.info("发送验证码成功，验证码：{}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();

        if ( cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("验证码错误");
        }

        User user = query().eq("phone", phone).one();

        if (user == null) {
            user = createUserWithPhone(phone);
        }

        String token = UUID.randomUUID().toString(false);
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        /**
         * userDTO中含有long属性字段，需要将其转换成String类型，
         * 才可以使用stringRedisTemplate进行序列化
         */
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<String, Object>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1、获取当前登录用户id
        Long userId = UserHolder.getUser().getId();
        // 2、获取当前日期
        LocalDateTime now = LocalDateTime.now();
        // 3、拼接key(要从dataTime中获取年月的字符串格式)
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyy/MM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4、确定今天是这个月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5、将签到信息写入到redis中 SETBIT key offset 0 / 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1、获取当前登录用户id
        Long userId = UserHolder.getUser().getId();
        // 2、获取当前日期
        LocalDateTime now = LocalDateTime.now();
        // 3、拼接key(要从dataTime中获取年月的字符串格式)
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyy/MM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4、确定今天是这个月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5、获取本月截至到今天为止的所有签到记录，返回的是一个十进制的数字  BITFIELD key GET u[第几天] offset（开始位置）
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        // 判断是否有签到记录
        if (result == null || result.isEmpty()) {
            // 没有签到结果，直接返回
            return Result.fail("没有任何签到结果");
        }
        // 获取签到结果
        Long num = result.get(0);
        // 判断签到结果
        if (num == null || num == 0) {
            // 签到数据不存在
            return Result.ok(0);
        }
        // 6、循环遍历
        int count = 0;
        while (true) {
            // 6.1、让获取的数字与1做与运算，得到数字的最后一个bit位，并且判断这个bit位是否为0
            if ((num & 1) == 0) {
                // 6.2、为0，未签到，结束
                break;
            } else {
                // 6.3、不为0，说明已经签到，计数器 + 1
                count++;
            }
            // 6.4、让数字向右移动一位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
