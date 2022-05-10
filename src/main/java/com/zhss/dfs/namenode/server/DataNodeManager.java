package com.zhss.dfs.namenode.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这个组件，就是负责管理集群里的所有的datanode的
 *
 * @author zhonghuashishan
 */
public class DataNodeManager {

    private Map<String, DataNodeInfo> datanodes = new ConcurrentHashMap<>();

    public DataNodeManager() {
        new DataNodeAliveMonitor().start();
    }

    /**
     * datanode进行注册
     *
     * @param ip
     * @param hostname
     */
    public Boolean register(String ip, String hostname) {
        DataNodeInfo datanode = new DataNodeInfo(ip, hostname);
        datanodes.put(ip + "-" + hostname, datanode);
        return true;
    }

    /**
     * datanode进行心跳
     *
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean heartbeat(String ip, String hostname) {
        DataNodeInfo datanode = datanodes.get(ip + "-" + hostname);
        datanode.setLatestHeartbeatTime(System.currentTimeMillis());
        return true;
    }

    /**
     * datanode是否存活的监控线程
     */
    class DataNodeAliveMonitor extends Thread {

        @Override
        public void run() {
            List<String> toRemoveDataNodes = new ArrayList<>();
            try {
                Iterator<DataNodeInfo> dataNodesIterator = datanodes.values().iterator();
                DataNodeInfo dataNode = null;
                while (dataNodesIterator.hasNext()) {
                    dataNode = dataNodesIterator.next();
                    if (System.currentTimeMillis() - dataNode.getLatestHeartbeatTime() > 90 * 1000) {
                        toRemoveDataNodes.add(dataNode.getIp() + "-" + dataNode.getHostname());
                    }
                }
                if (!toRemoveDataNodes.isEmpty()) {
                    for (String toRemoveDataNode : toRemoveDataNodes) {
                        datanodes.remove(toRemoveDataNode);
                    }
                }
                Thread.sleep(30 * 1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
