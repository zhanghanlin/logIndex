package com.cheyipai.elasticsearch.common.listener;

import com.cheyipai.elasticsearch.common.config.Global;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.ServletContext;

public class WebContextListener extends ContextLoaderListener {

    @Override
    public WebApplicationContext initWebApplicationContext(ServletContext servletContext) {
        if (!printKeyLoadMessage()) {
            return null;
        }
        return super.initWebApplicationContext(servletContext);
    }

    /**
     * 获取Key加载信息
     */
    static boolean printKeyLoadMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\r\n======================================================================\r\n");
        sb.append("\r\n    欢迎使用 " + Global.getConfig("productName") + "\r\n");
        sb.append("\r\n======================================================================\r\n");
        System.out.println(sb.toString());
        return true;
    }
}
