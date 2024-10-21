package com.hmdp;

import com.fasterxml.jackson.datatype.jsr310.deser.LocalTimeDeserializer;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.LocalTime;

@SpringBootTest
public class AddSeckillVoucher {

    @Resource
    IVoucherService voucherService;

    @Test
    void addSeckillVoucher() {
        Voucher voucher = new Voucher();
        voucher.setId(3L);
        voucher.setShopId(1L);
        voucher.setStock(100);
        voucher.setType(1);
        voucher.setRules("全场通用");
        voucher.setTitle("100元代金券");
        voucher.setSubTitle("haha");
        voucher.setActualValue(10000L);
        voucher.setPayValue(8000L);
        voucher.setStatus(1);
        voucher.setCreateTime(LocalDateTime.now());
        voucher.setUpdateTime(LocalDateTime.now());
        voucher.setBeginTime(LocalDateTime.now().with(LocalTime.of(10,0)));
        voucher.setEndTime(LocalDateTime.now().with(LocalTime.of(23,39)));
        voucherService.addSeckillVoucher(voucher);
    }
}
