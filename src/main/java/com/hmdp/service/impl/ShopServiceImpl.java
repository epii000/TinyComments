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
import com.hmdp.redis.CacheClient;
import com.hmdp.redis.RedisConstants;
import com.hmdp.redis.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoLocation;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.redis.RedisConstants.SHOP_GEO_KEY;

/**
 * <p>
 * 服务实现类
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
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存穿透
        // Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存穿透
//        Shop shop = queryWithLogicalExpire(id);

//        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById,
//                RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    public Shop queryWithMutex(Long id) {
        // 1.从redis查询店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if (shopJson != null) {
            return null;
        }

        // 4.实现缓存重建,使用互斥锁来避免缓存击穿
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            shop = getById(id);
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "",
                        RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
            }
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),
                    RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
        // 从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 不存在直接返回空值
            return null;
        }
        // 命中，将json反序列化成对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，则直接返回结果
            return shop;
        }
        // 缓存重建
        // a.获取互斥锁，
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // b.开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                // 缓存重建
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        return null;
    }

    public Shop queryWithPassThrough(Long id) {
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
        //从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断空值
        if (shopJson != null) {
            return null;
        }
        //不存在 查询数据库
        Shop shop = getById(id);
        if (shop == null) {
            // redis写入空值
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "",
                    RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 数据库不存在 返回错误
            return null;
        }
        //数据库存在 写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),
                RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空！");
        }
        // 先更新数据库，再删除缓存
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis，按照距离分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000), // 周围5000米范围
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4、获取集合，解析出shopId
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        // 判断集合和分页头的大小，如果list小于from，skip跳过后就没内容了，就会报空指针异常
        if (list.size() <= from) {
            // 没有下一页，不能滚动翻页了，直接返回
            return Result.ok(Collections.emptyList());
        }
        // 4.1、截取从from ~ end的部分(subList、或者stream流都可以，这里用stream流的方式：不需要拷贝集合，只是跳过，更加节省内存)
        // 通过list列表去收集后面的店铺id
        List<Long> ids = new ArrayList<>(list.size());
        // 通过Map集合让id和distance一一对应(key: id字符串，value：Distance)
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> { // 跳过再遍历
            // 4.2、获取店铺id
            String shopIdStr = result.getContent().getName();
            // 4.3、将id转换为Long型，存入用于后面查询获得对象
            ids.add(Long.valueOf(shopIdStr));
            // 4.4、获取店铺到当前用户的距离
            Distance distance = result.getDistance();
            // 4.5、id和distance做匹配，一起返回
            distanceMap.put(shopIdStr, distance);

        });
        // 5、根据shopId查询数据库获得shop对象
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        // 6、将店铺和距离一一对应合并在一起
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 7、返回
        return Result.ok(shops);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) {
        // 查询店铺数据，封装逻辑过期时间，写入redis
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
