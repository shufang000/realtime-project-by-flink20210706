package com.shufang.flinkapp.avro.schemas;

import com.shufang.flinkapp.avro.bean.User;
import com.shufang.flinkapp.avro.bean.User1;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 该类主要是实现Kafka数据的
 */
public class MyAvroSchema implements SerializationSchema<User> , DeserializationSchema<User1> {
    @Override
    public User1 deserialize(byte[] message) throws IOException {
        // 用来保存结果数据
        User1 user = new User1();
        // 创建输入流用来读取二进制文件
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(message);
        // 创建输入序列化执行器
        SpecificDatumReader<User1> stockSpecificDatumReader = new SpecificDatumReader<User1>(user.getSchema());
        //创建二进制解码器
        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(arrayInputStream, null);
        try {
            // 数据读取
            user=stockSpecificDatumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 结果返回
        return user;
    }

    @Override
    public boolean isEndOfStream(User1 nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(User user) {
        // 创建序列化执行器
        SpecificDatumWriter<User> writer = new SpecificDatumWriter<User>(user.getSchema());
        // 创建一个流 用存储序列化后的二进制文件
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 创建二进制编码器
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            // 数据入都流中
            writer.write(user, encoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    @Override
    public TypeInformation<User1> getProducedType() {
        return TypeInformation.of(User1.class);
    }
}
