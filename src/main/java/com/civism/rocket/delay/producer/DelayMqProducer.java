package com.civism.rocket.delay.producer;

import com.civism.rocket.delay.calculate.DelayLevelCalculate;
import com.civism.rocket.delay.constant.GuavaRocketConstants;
import com.civism.rocket.delay.listener.GuavaMessageListener;
import com.civism.rocket.delay.wheel.SendRealMqTask;
import com.civism.rocket.delay.wheel.TimeWheelFactory;
import io.netty.util.HashedWheelTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author : Guava
 * @version 1.0
 * @projectName：civism-rocket
 * @className：DelayMQProducer
 * @date 2020/1/7 2:12 下午
 * @return
 */
@Slf4j
public class DelayMqProducer extends DefaultMQProducer {

    private DefaultMQPushConsumer defaultMQPushConsumer;

    public DelayMqProducer(String producerGroup) {
        super(producerGroup);
    }

    /**
     * 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h rocketMQ自动支持18个级别 等级全部转化为秒
     *
     * 延时小于1分钟的 不支持，可以设置强制参数来支持，但是会有误差
     */
    public SendResult sendDelay(Message msg, Date startSendTime) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        if (startSendTime == null) {
            return super.send(msg);
        }
        long l = Duration.between(Instant.now(), startSendTime.toInstant()).getSeconds();
        //如果不等于0,说明设置了延时等级，直接用rocketMQ支持的发送
        if (l <= GuavaRocketConstants.TIME_OUT) {
            HashedWheelTimer instance = TimeWheelFactory.getInstance();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            SendRealMqTask sendRealMqTask = new SendRealMqTask();
            sendRealMqTask.setDelayMqProducer(this);
            sendRealMqTask.setMessage(msg);
            sendRealMqTask.setCountDownLatch(countDownLatch);
            instance.newTimeout(sendRealMqTask, l < 0 ? 1 : l, TimeUnit.SECONDS);
            countDownLatch.await();
            return sendRealMqTask.getResult();
        } else {
            Integer level = DelayLevelCalculate.calculateDefault(l);
            fillMessage(msg, level, startSendTime);
            return super.send(msg);
        }
    }

    @Override
    public void start() throws MQClientException {
        super.start();
        initConsumer();
    }

    private void initConsumer() throws MQClientException {
        defaultMQPushConsumer = new DefaultMQPushConsumer(super.getProducerGroup());
        defaultMQPushConsumer.setNamesrvAddr(super.getNamesrvAddr());
        defaultMQPushConsumer.setMessageListener(new GuavaMessageListener(this));
        defaultMQPushConsumer.subscribe(GuavaRocketConstants.PROXY_TOPIC, "*");
        defaultMQPushConsumer.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (defaultMQPushConsumer != null) {
            defaultMQPushConsumer.shutdown();
        }
    }

    /**
     * 填充消息
     *
     * @param msg 消息体
     * @param level 延时等级
     * @param startSendTime 发送时间
     */
    private void fillMessage(Message msg, Integer level, Date startSendTime) {
        msg.putUserProperty(GuavaRocketConstants.GUAVA_TIMES, String.valueOf(startSendTime.getTime() / 1000 - DelayLevelCalculate.get(level - 1)));

        String topic = msg.getProperty(GuavaRocketConstants.GUAVA_ORIGINAL_TOPIC);
        if (StringUtils.isBlank(topic)) {
            msg.putUserProperty(GuavaRocketConstants.GUAVA_ORIGINAL_TOPIC, msg.getTopic());
        }
        String tag = msg.getProperty(GuavaRocketConstants.GUAVA_ORIGINAL_TAG);
        if (StringUtils.isBlank(tag)) {
            if (StringUtils.isNotBlank(msg.getTags())) {
                msg.putUserProperty(GuavaRocketConstants.GUAVA_ORIGINAL_TAG, msg.getTags());
            }
        }
        String keys = msg.getProperty(GuavaRocketConstants.GUAVA_ORIGINAL_KEY);
        if (StringUtils.isBlank(keys)) {
            if (StringUtils.isNotBlank(msg.getKeys())) {
                msg.putUserProperty(GuavaRocketConstants.GUAVA_ORIGINAL_KEY, msg.getKeys());
            }
        }

        String property = msg.getProperty(GuavaRocketConstants.GUAVA_ORIGINAL_UUID);
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        if (StringUtils.isBlank(property)) {
            msg.putUserProperty(GuavaRocketConstants.GUAVA_ORIGINAL_UUID, uuid);
        }
        msg.setDelayTimeLevel(level);
        msg.setTopic(GuavaRocketConstants.PROXY_TOPIC);
        log.info("消息uuid {} 开发发送时间为{},延时等级本次建议为{}", uuid, String.format("%tF %<tT", new Date()), level);
    }
}
