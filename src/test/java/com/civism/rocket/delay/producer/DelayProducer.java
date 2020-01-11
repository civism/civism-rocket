package com.civism.rocket.delay.producer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author : Guava
 * @version 1.0
 * @projectName：civism-rocket
 * @className：DelayProducer
 * @date 2019-12-25 11:36
 * @E-mail:gongdexing@oxyzgroup.com
 * @Copyright: 版权所有 (C) 2019 蓝鲸淘.
 * @return
 */
public class DelayProducer {

    public static void main(String[] args) throws Exception {
        DelayMqProducer producer = new DelayMqProducer("guava-group");
        producer.setNamesrvAddr("namesrv1.bw365.net:9876;namesrv2.bw365.net:9876");
        producer.setRetryTimesWhenSendFailed(3);
        producer.start();
        Map<String, String> content = new HashMap<>();
        content.put("name", "guava");
        content.put("message", "hello word");
        Message message = new Message("guava_hello_topic", null, JSON.toJSONBytes(content));
        SendResult sendResult = producer.sendDelay(message, DateUtils.addSeconds(new Date(), 89));
        System.out.println("发送返回结果：" + JSON.toJSONString(sendResult));
        System.out.println("消息发送时间：" + String.format("%tF %<tT", new Date()));
    }

}
