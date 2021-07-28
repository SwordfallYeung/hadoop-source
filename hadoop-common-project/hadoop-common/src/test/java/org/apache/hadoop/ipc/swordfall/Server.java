package org.apache.hadoop.ipc.swordfall;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 启动RPC服务
 */
public class Server {

    public static void main(String[] args) throws Exception {
        // 1. 构建RPC框架
        RPC.Builder builder = new RPC.Builder(new Configuration());
        // 2. 绑定地址
        builder.setBindAddress("localhost");
        // 3. 绑定端口
        builder.setPort(7777);
        // 4. 绑定协议
        builder.setProtocol(ClientNameNodeProtocol.class);
        // 5. 调用协议实现类
        builder.setInstance(new ClientNameNodeImpl());
        // 6. 创建服务
        RPC.Server server = builder.build();
        // 7. 启动服务
        server.start();
    }
}
