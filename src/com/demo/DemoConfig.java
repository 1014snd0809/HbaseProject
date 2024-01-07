package com.demo;

import com.jfinal.config.Constants;
import com.jfinal.config.Handlers;
import com.jfinal.config.Interceptors;
import com.jfinal.config.JFinalConfig;
import com.jfinal.config.Plugins;
import com.jfinal.config.Routes;
import com.jfinal.template.Engine;
import com.plugin.HbasePlugin;

public class DemoConfig extends JFinalConfig{

    @Override
    public void configConstant(Constants me) {
        me.setDevMode(true);
    }
    @Override
    public void configRoute(Routes me) {
        me.setBaseViewPath("/WEB-INF");
        me.add("/hello", HelloController.class,"main");
    }
    @Override
    public void configEngine(Engine me) {
    }
    @Override
    public void configPlugin(Plugins me) {
        //添加hbase插件
        String quorum="node1,node2,node3";//这里是你虚拟机的ip
        HbasePlugin hp=new HbasePlugin(quorum);
        me.add(hp);
    }
    @Override
    public void configInterceptor(Interceptors me) {
        // TODO Auto-generated method stub
    }
    @Override
    public void configHandler(Handlers me) {
        // TODO Auto-generated method stub
    }
}

