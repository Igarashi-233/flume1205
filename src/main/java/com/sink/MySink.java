package com.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    //定义前后缀
    private String prefix;
    private String suffix;

    //获取logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);

    public void configure(Context context) {

        //读取配置文件给前后缀赋值
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "IGARASHI");

    }


    /**
     * 1.获取channel
     * 2.从channel种获取事务以及数据
     * 3.发送数据
     *
     */
    public Status process() throws EventDeliveryException {

        Status status = null;
        //1.获取
        Channel channel = getChannel();

        //2.获取事务
        Transaction transaction = channel.getTransaction();

        //3.开启事务
        transaction.begin();

        try {
            //4.获取数据
            Event event = channel.take();

            //5.处理事件
            if (event != null){
                String body = new String(event.getBody());
                logger.info(prefix + body + suffix);
            }

            //6.提交事务
            transaction.commit();

            status = Status.READY;

        } catch (ChannelException e) {
            e.printStackTrace();

            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            //关闭事务
            transaction.close();
        }

        return status;
    }

}
