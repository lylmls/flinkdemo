package com.flume;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class FlumeETLInterept implements Interceptor{

    public void initialize() {

    }

    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body);

        if (log.contains("start")) {
            NumberUtils.isNumber(log);
//            LogUtil.validte
        }
        return null;
    }

    public List<Event> intercept(List<Event> list) {
        return null;
    }

    public void close() {

    }
}
