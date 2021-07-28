package org.apache.hadoop.ipc.swordfall;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * 访问RPC服务
 */
public class Client {

    public static void main(String[] args) throws Exception {
        // 1. 拿到RPC协议
        ClientNameNodeProtocol proxy = RPC.getProxy(ClientNameNodeProtocol.class, 1L, new InetSocketAddress("localhost", 7777), new Configuration());
        // 2. 发送请求
        String metaData = proxy.getMetaData("/meta");
        // 3. 打印元数据
        System.out.println(metaData);
    }
}
