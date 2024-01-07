package com.plugin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.jfinal.plugin.IPlugin;

public class HbasePlugin implements IPlugin{

    private String quorum;

    public HbasePlugin(String  quorum) {
        this.quorum = quorum;
    }
    @Override
    public boolean start() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            Hbase.connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    @Override
    public boolean stop() {
        if (!Hbase.connection.isClosed()) {
            try {
                Hbase.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }
}

