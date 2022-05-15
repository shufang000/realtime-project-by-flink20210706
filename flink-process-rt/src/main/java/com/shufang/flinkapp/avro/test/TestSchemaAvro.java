package com.shufang.flinkapp.avro.test;

import com.shufang.flinkapp.avro.bean.User;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class TestSchemaAvro {
    public static void main(String[] args) {

        // 获取到bean的json格式的avroschema，可以在 hudi 的delta streamer使用
        System.out.println(new ReflectData().getSchema(User.class).toString());
    }
}
