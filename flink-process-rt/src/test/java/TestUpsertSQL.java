import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.common.CommonConfig;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

public class TestUpsertSQL {
    @Test
    public void testConcatSQL(){

        String jsonStr = "{\"id\":\"1001\",\"name\":\"shufuang\",\"sink_table\":\"SINKTABLE\"}";
        JSONObject jsonObject = JSON.parseObject(jsonStr);

        String sinkTable = jsonObject.getString("sink_table");
        System.out.println(jsonObject);
        Set<String> columns = jsonObject.keySet();
        Collection<Object> values = jsonObject.values();
        //TODO 这里使用apache common的StringUtils工具类进行SQL的拼接
        String columnsString = StringUtils.join(columns, ",");
        String valuesStr = StringUtils.join(values, "','");
        //UPSERT INTO HBASE_SHUFANG.SINKTABLE(name,sink_table,id) VALUES ('shufuang','SINKTABLE','1001')
        String upsertSQL = "UPSERT INTO " + CommonConfig.HBASE_NAMESPACE + "." + sinkTable + "(" +
                columnsString +
                ") VALUES ('" + valuesStr + "')";


        System.out.println(upsertSQL);

    }
}
