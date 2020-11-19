package com.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    //声明一个存放事件的集合
    private List<Event> addHeaderEvents;

    public void initialize() {

        //初始化
        addHeaderEvents = new ArrayList<Event>();

    }

    //单个事件拦截
    public Event intercept(Event event) {

        //获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        //获取事件中的body信息
        String body = new String(event.getBody());

        //根据body中是否有“hello”来决定添加怎样的头信息
        if (body.contains("hello")) {

            //添加头信息(整合KAFKA)
            headers.put("topic", "first");

        } else {

            //添加头信息
            headers.put("topic", "second");

        }

        return event;
    }

    //批量事件拦截
    public List<Event> intercept(List<Event> events) {

        //清空集合
        addHeaderEvents.clear();

        //遍历events, 给每一个事件添加头信息
        for (Event event : events) {

            Event intercept = intercept(event);
            addHeaderEvents.add(intercept);

        }

        //返回结果
        return addHeaderEvents;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }

}
