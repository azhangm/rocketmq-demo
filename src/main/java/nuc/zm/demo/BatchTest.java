package nuc.zm.demo;

import nuc.zm.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 批量测试
 *
 * @author zm
 */
public class BatchTest {

    /**
     * 批量生产
     */
    @Test
    public void batchProducer() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("batch-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQProducer.start();
        ArrayList<Message> list = new ArrayList<>();
        String topic = "batchTest" ;
        for (char a  = 'a' ; a < 'c' ; a ++) {
            list.add(new Message(topic,("我是一组消息中的" + a + "消息").getBytes(StandardCharsets.UTF_8)));
        }
        SendResult send = defaultMQProducer.send(list);
        System.out.println(send.getSendStatus());
        defaultMQProducer.shutdown();
    }
    @Test
    public void msConsumer() throws MQClientException, IOException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("batch-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQPushConsumer.subscribe("batchTest","*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("收到消息了" + new Date());
                System.out.println("收到消息的数组大小" + list.size());
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
