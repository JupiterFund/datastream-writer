package com.nodeunify.jupiter.datastream.writer;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.nodeunify.jupiter.datastream.v1.FutureData;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// import lombok.extern.slf4j.Slf4j;

// @Slf4j
@Service
public class FutureDataWriter {

    @Value("${app.parquet.dir-path}")
    private String dirPath;

    private Path path;
    private ProtoParquetWriter<FutureData> writer;

    @PostConstruct
    public void postConstruct() {
        String dateToday = Util.getDateOfToday();
        String filePath = dirPath + "data_" + dateToday + ".futuredata.parquet";
        path = new Path(filePath);
        try {
            writer = new ProtoParquetWriter<FutureData>(path, FutureData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void preDestroy() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics = "${spring.kafka.topic.future-data}")
    public void writeFutureData(byte[] bytes) {
        try {
            FutureData futureData = FutureData.parseFrom(bytes);
            // log.debug("futureData: {}", futureData.getCode());
            writer.write(futureData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}