import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.bean.OrderWide;
import org.junit.Test;


public class Test1 {
    @Test
    public void test1() throws Exception {

        String s = "{\n" +
        "\t\"activity_reduce_amount\": 700.00,\n" +
        "\t\"category3_id\": 86,\n" +
        "\t\"category3_name\": \"平板电视\",\n" +
        "\t\"coupon_reduce_amount\": 0.00,\n" +
        "\t\"create_time\": \"2021-07-25 22:26:05\",\n" +
        "\t\"detail_id\": 284798,\n" +
        "\t\"feight_fee\": 19.00,\n" +
        "\t\"order_id\": 92217,\n" +
        "\t\"order_price\": 6699.00,\n" +
        "\t\"order_status\": \"1001\",\n" +
        "\t\"original_total_amount\": 40160.00,\n" +
        "\t\"province_3166_2_code\": \"CN-JL\",\n" +
        "\t\"province_area_code\": \"220000\",\n" +
        "\t\"province_id\": 16,\n" +
        "\t\"province_iso_code\": \"CN-22\",\n" +
        "\t\"province_name\": \"吉林\",\n" +
        "\t\"sku_id\": 17,\n" +
        "\t\"sku_name\": \"TCL 65Q10 65英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视\",\n" +
        "\t\"sku_num\": 2,\n" +
        "\t\"split_total_amount\": 13398.00,\n" +
        "\t\"spu_id\": 5,\n" +
        "\t\"spu_name\": \"TCL巨幕私人影院电视 4K超高清 AI智慧屏  液晶平板电视机\",\n" +
        "\t\"tm_id\": 4,\n" +
        "\t\"tm_name\": \"TCL\",\n" +
        "\t\"total_amount\": 39479.00,\n" +
        "\t\"user_age\": 40,\n" +
        "\t\"user_gender\": \"F\",\n" +
        "\t\"user_id\": 4437\n" +
        "}";

        // 这个方法需要调用该Bean的有参构造器，不然属性全部为null
        OrderWide orderWide = JSONObject.parseObject(s, OrderWide.class);
        System.out.println(orderWide);


        System.out.println(JSONObject.toJSONString(orderWide));
    }
}
