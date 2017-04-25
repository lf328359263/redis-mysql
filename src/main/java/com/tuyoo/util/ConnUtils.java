package com.tuyoo.util;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 连接工具类
 */
public class ConnUtils {

    private static Logger LOG = Logger.getLogger(ConnUtils.class);
    private Properties properties;

    public ConnUtils (Properties properties) {
        this.properties = properties;
    }

    public Connection getMysqlConn(){
        try {
            LOG.info("获取mysql连接。。。");
            Class.forName(properties.getProperty("mysql.driver"));
            return DriverManager.getConnection(properties.getProperty("mysql.url"), properties.getProperty("mysql.user"), properties.getProperty("mysql.password"));
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            return null;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            return null;
        }
    }

    public Jedis getJedis(){
        LOG.info("获取redis连接。。。");
        Jedis jedis = new Jedis(properties.getProperty("redis.host"), Integer.parseInt(properties.getProperty("redis.port")));
        if(properties.containsKey("redis.password")){
            LOG.info("密码认证。。");
            jedis.auth(properties.getProperty("redis.password"));
        }
        return jedis;
    }

    private void insertBatch(String sql, Set<List<String>> result) {
        int len = sql.split("\\?").length-1;
        LOG.info("执行插入的sql语句: " + sql);
        Connection conn = getMysqlConn();
        PreparedStatement psm = null;
        try {
            conn.setAutoCommit(false);
            psm = conn.prepareStatement(sql);
            LOG.info("添加批量执行...");
            for (List<String> temp : result) {
                if (temp.size() == len) {
                    for (int i = 1; i <= len; i++) {
                        psm.setObject(i, temp.get(i - 1));
                    }
                    psm.addBatch();
                    temp.clear();
                }
            }
            LOG.info("执行批量插入...");
            psm.executeBatch();
            psm.clearBatch();
            conn.commit();
            LOG.info("本次提交完成...");
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
               LOG.error(e.getMessage());
            }
        }
    }

//    public void payInsertBatch(Set<List<String>> result) {
//        String sql = "insert into redis_pay(type,platform_id,product_id,channel_id,product_nickname,client_id,prod_id,pay_type,value,day,hour,minute) values(?,?,?,?,?,?,?,?,?,?,?,?)";
//        insertBatch(sql, result);
//    }

    public void loginInsertBatch(Set<List<String>> result){
        String sql = "insert into redis_login(type,platform_id,product_id,channel_id,product_nickname,client_id,value,day,hour,minute) values(?,?,?,?,?,?,?,?,?,?)";
        insertBatch(sql, result);
    }

    public void gameInsertBatch(Set<List<String>> result){
        String sql = "insert into redis_game(type,platform_id,product_id,game_id,channel_id,product_nickname,client_id,value,day,hour,minute) values(?,?,?,?,?,?,?,?,?,?,?)";
        insertBatch(sql, result);
    }

    public void loginInsertBatchShort(Set<List<String>> result){
        String sql = "insert into redis_short_login(type,platform_id,product_id,channel_id,product_nickname,client_id,value,day,hour,minute) values(?,?,?,?,?,?,?,?,?,?)";
        insertBatch(sql, result);
    }

    public void gameInsertBatchShort(Set<List<String>> result){
        String sql = "insert into redis_short_game(type,platform_id,product_id,game_id,channel_id,product_nickname,client_id,value,day,hour,minute) values(?,?,?,?,?,?,?,?,?,?,?)";
        insertBatch(sql, result);
    }

}
