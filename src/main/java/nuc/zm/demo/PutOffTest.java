package nuc.zm.demo;

import nuc.zm.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

/**
 * 延迟消费
 *
 * @author zm
 * @date 2023/06/12
 */
public class PutOffTest {

    /**
     * 推迟生产
     */
    @Test
    public void putOffProducer() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("delay-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQProducer.start();
        Message message = new Message("orderMsTopic","延迟消息".getBytes(StandardCharsets.UTF_8));
        // 10s 后收到 在配置文件中可以去指定 等级对应的时间
        message.setDelayTimeLevel(3);
        defaultMQProducer.send(message);
        System.out.println("发送时间" + new Date() );
        defaultMQProducer.shutdown();
    }

    /**
     * 延迟消费者
     * 第一次测试 ---> 比较慢 可能在加载一些东西
     * 发送时间 Mon Jun 12 15:27:12 CST 2023
     * 收到时间 Mon Jun 12 15:27:49 CST 2023
     * =====================================
     * 第二次就快了
     * =====================================
     * 发送时间 Mon Jun 12 15:30:20 CST 2023
     * 收到时间 Mon Jun 12 15:30:30 CST 2023
     */
    @Test
    public void msConsumer() throws MQClientException, IOException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("delay-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQPushConsumer.subscribe("orderMsTopic","*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("收到消息了" + new Date());
                byte[] body = list.get(0).getBody();
                String s = new String(body);
                System.out.println(s);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        defaultMQPushConsumer.start();
        System.in.read();
    }

}
