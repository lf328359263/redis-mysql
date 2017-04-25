package com.tuyoo.action;

import com.tuyoo.util.ConnUtils;
import com.tuyoo.util.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 主程序
 */
public class MainAction {

    public static Logger LOG = Logger.getLogger(MainAction.class);

    public static void main(String[] args) {
        String propertiesHome = "E:\\mysite\\ideaProjects\\redis-mysql\\src\\main\\resources";
        if(args.length != 1){
            System.out.println("参数： <path:配置文件地址>");
            return;
        } else {
            propertiesHome = args[0];
        }
        propertiesHome = propertiesHome.endsWith("/") ? propertiesHome : propertiesHome + "/";
        PropertyConfigurator.configure(propertiesHome+"log4j.properties");
        Properties connProperties = Utils.getProperties(propertiesHome+"conn.properties");
        ConnUtils connUtil = new ConnUtils(connProperties);

        exe(connUtil, new Date(), true);
        exe(connUtil, new Date(), false);

    }

    public static void exe(ConnUtils conn, Date d, boolean delete){
        Jedis jedis = conn.getJedis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH mm");
        String[] format = sdf.format(d).split(" ");
        ArrayList<String> dhm = new ArrayList<String>(Arrays.asList(format));

        Set<List<String>> loginValues = new HashSet<List<String>>();
        Set<List<String>> gameValues = new HashSet<List<String>>();

        if(delete){
            jedis.select(40);
        } else {
            int db = Integer.parseInt(format[0].split("-")[2]);
            jedis.select(db);
        }

        Pipeline pipeline = jedis.pipelined();
        Map<String, Response<Long>> result = new HashMap<String, Response<Long>>();

        long selectStart = System.currentTimeMillis();
        Set<String> keys = jedis.keys("*");
        for(String key : keys) {
            if (key.startsWith("1") || key.startsWith("2") || key.startsWith("6")) {
                result.put(key, pipeline.scard(key));
            }
        }
        pipeline.sync();
        LOG.info("redis 查询用时(秒数)： "+(System.currentTimeMillis()-selectStart));
        if(delete){
            LOG.info("清空数据。。"+jedis.getDB());
            jedis.flushDB();
        }
        jedis.close();

        for(Map.Entry<String, Response<Long>> entry : result.entrySet()){
            List<String> temp = Utils.parseToList(entry.getKey());
            temp.add(entry.getValue().get().toString());
            temp.addAll(dhm);
            if(entry.getKey().startsWith("2")){
                gameValues.add(temp);
            } else {
                loginValues.add(temp);
            }
        }

        if(delete){
            LOG.info("当前十分钟数据写入：game->"+gameValues.size() + "\t login->"+loginValues.size());
            conn.loginInsertBatchShort(loginValues);
            conn.gameInsertBatchShort(gameValues);
        } else {
            LOG.info("累计数据写入：game->"+gameValues.size() + "\t login->"+loginValues.size());
            conn.gameInsertBatch(gameValues);
            conn.loginInsertBatch(loginValues);
        }
    }

}
