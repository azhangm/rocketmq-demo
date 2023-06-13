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
 * 顺序发送
 *
 * @author zm
 */
public class DMSTest {

    @Test
    public void orderProducer() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("order-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        for (int i = 0; i < 100; i++) {
            Message orderTopic = new Message("orderTopic", ("我是第" + i + "个消息").getBytes(StandardCharsets.UTF_8));
            producer.send(orderTopic);

        }
        producer.shutdown();
        System.out.println("发送完毕");
    }

    /**
     * 为消费者
     * === 乱序消费 === 不保证全局 FIFO
     * 我是第77个消息
     * 我是第43个消息
     * 我是第59个消息
     * 我是第79个消息
     * 我是第83个消息
     */
    @Test
    public void orderConsumer() throws MQClientException, IOException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("order-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQPushConsumer.subscribe("orderTopic","*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
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
