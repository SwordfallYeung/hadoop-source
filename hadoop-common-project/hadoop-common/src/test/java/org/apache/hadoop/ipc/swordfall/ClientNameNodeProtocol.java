package org.apache.hadoop.ipc.swordfall;

/**
 * todo 在这里RPC实现其实就是分三部分，分别是协议定义（接口）&实现，Server端实现和Client实现三个部分。
 *
 * todo 第一部分定义协议：其实就是根据业务需要定义个接口协议。
 * 协议接口
 */
public interface ClientNameNodeProtocol {
    // 1. 定义协议的ID
    public static final long versionID = 1L;

    /**
     * 拿到元数据方法，协议通信前会访问元数据
     */
    public String getMetaData(String path);
}
