package nuc.zm.demo;

import nuc.zm.constant.MqConstant;
import nuc.zm.demo.domain.Msg;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * forderly测试
 *
 * @author zm
 * @date 2023/06/13
 */
public class FOrderlyTest {

    private List<Msg> list = Arrays.asList(
            new Msg(1,1,"下单"),
            new Msg(2,2,"下单"),
            new Msg(1,1,"短信"),
            new Msg(2,2,"短信"),
            new Msg(1,1,"物流"),
            new Msg(2,2,"物流")
    );

    @Test
    public void orderProducer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("order-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        // 发送顺序 消息 ， 发送时确保有序，并且要发到同一个队列里面去。
        list.forEach(msg -> {
            Message orderTopic = new Message("orderTopic", msg.toString().getBytes(StandardCharsets.UTF_8));

            try {
                producer.send(orderTopic, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        int idx = o.hashCode() % list.size();
//                        int idx = o.toString().hashCode() % list.size();
                        System.out.println("第 " +  idx + "个队列");
                        return list.get(idx);
                    }
                },msg.getOrderId());
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        producer.shutdown();
        System.out.println("发送完毕");
    }


    @Test
    public void orderConsumer() throws MQClientException, IOException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("orderTopic","*") ;
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                System.out.println("消息上下文"  + consumeOrderlyContext);
                System.out.println("当前消息" + list.get(0));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }
}
