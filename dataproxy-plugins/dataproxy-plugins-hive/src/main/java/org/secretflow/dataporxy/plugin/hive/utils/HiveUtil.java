package org.secretflow.dataporxy.plugin.hive.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.secretflow.dataporxy.plugin.hive.config.HiveConnectConfig;

public class HiveUtil {
    
    public static Connection initHive(HiveConnectConfig config) throws SQLException {
        String endpoint = config.endpoint();
        String ip;
        int port = 10000; // 默认端口

        if (endpoint.contains(":")) {
            String[] parts = endpoint.split(":");
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                port = Integer.parseInt(parts[1]);
            }
        } else {
            ip = endpoint;
        }
        Connection conn;
        try{
            conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%s/%s", ip, port, config.database()), config.username(), config.password());
        } catch (Exception e) {
            System.out.println("hive init error");
            throw new RuntimeException(e);
        }

        return conn;


    }
}
