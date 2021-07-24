import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.bean.OrderDetail;
import com.shufang.flinkapp.bean.OrderInfo;
import com.shufang.flinkapp.common.CommonConfig;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestTimer {


    public static void main(String[] args) {

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("helloworld == " + System.currentTimeMillis());
            }
        }, 5000, 5000);
    }

    @Test
    public void testJsonRemove() {

        String json = "{\n" +
                "\"database\":\"realtime\",\n" +
                "\"table\":\"cart_info\",\n" +
                "\"type\":\"delete\",\n" +
                "\"ts\":1625836525,\n" +
                "\"xid\":264,\n" +
                "\"xoffset\":18553,\n" +
                "\"data\":{\n" +
                "\"id\":205476,\n" +
                "\"user_id\":\"7246\",\n" +
                "\"sku_id\":31,\n" +
                "\"cart_price\":69.00,\n" +
                "\"sku_num\":2,\n" +
                "\"img_url\":\"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-0y1WATxItAAEcZnKxvfI617.jpg\",\n" +
                "\"sku_name\":\"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M03赤茶\",\n" +
                "\"is_checked\":null,\n" +
                "\"create_time\":\"2021-07-08 23:26:39\",\n" +
                "\"operate_time\":null,\n" +
                "\"is_ordered\":1,\n" +
                "\"order_time\":\"2021-07-08 23:28:04\",\n" +
                "\"source_type\":\"2404\",\n" +
                "\"source_id\":2\n" +
                "}\n" +
                "}";

        JSONObject jsonObject = JSON.parseObject(json);

        System.out.println("最开始的jsonObject = " + jsonObject);


        JSONObject data = jsonObject.getJSONObject("data");
        String sinkColumns = "id,user_id";


        List<String> sinkCols = Arrays.asList(sinkColumns.split(","));

        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        Map.Entry<String, Object> next = iterator.next();

        if (!sinkCols.contains(next.getKey())) {
            iterator.remove();
        }

        /**
         * java.util.ConcurrentModificationException
         * 	at java.util.HashMap$HashIterator.nextNode(HashMap.java:1437)
         * 	at java.util.HashMap$EntryIterator.next(HashMap.java:1471)
         * 	at java.util.HashMap$EntryIterator.next(HashMap.java:1469)
         * 	.........
         *
         Set<Map.Entry<String, Object>> entries = data.entrySet();
         for (Map.Entry<String, Object> entry : entries) {
         if (!sinkCols.contains(entry.getKey())){
         entries.remove(entry);
         }
         }
         */


        System.out.println("最终的jsonObject = " + jsonObject);


    }


    @Test
    public void testPhoenixConn() throws ClassNotFoundException, SQLException {
        System.setProperty("HADOOP_USER_NAME", "shufang");
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection(CommonConfig.PHOENIX_URL);
        System.out.println(conn);


    }


    @Test
    public void testJSONMap(){
        String json = "{\n" +
                "\t\"sku_num\": \"2\",\n" +
                "\t\"create_time\": \"2021-07-23 22:31:20\",\n" +
                "\t\"sku_id\": 5,\n" +
                "\t\"order_price\": 999.00,\n" +
                "\t\"source_type\": \"2402\",\n" +
                "\t\"sku_name\": \"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米\",\n" +
                "\t\"id\": 212967,\n" +
                "\t\"source_id\": 22,\n" +
                "\t\"order_id\": 69118,\n" +
                "\t\"split_total_amount\": 1998.00\n" +
                "}";


        System.out.println(JSON.parseObject(json, OrderDetail.class));


    }

    @Test
    public void test11() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(sdf.parse("2021-07-23 22:31:20").getTime());


        System.out.println(sdf.format(new Date(1627050680000L)));
    }
}
