package com.nodeunify.jupiter.datastream.writer;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.nodeunify.jupiter.datastream.v1.StockData;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class StockDataWriter {
        
    @Value("${app.parquet.dir-path.stock-data}")
    private String dirPath;

    // private Path path;
    // private ProtoParquetWriter<StockData> writer;

    @PostConstruct
    public void postConstruct() {
        // String dateToday = Util.getDateOfToday();
        // String filePath = dirPath + "data_" + dateToday + ".stockdata.parquet";
        // path = new Path(filePath);
        // try {
        //     writer = new ProtoParquetWriter<StockData>(path, StockData.class);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
    }

    @PreDestroy
    public void preDestroy() {
        System.out.println("preDestroy");
        // try {
        //     if (writer != null) {
        //         writer.close();
        //     }
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
    }

    // @KafkaListener(topics = "${spring.kafka.topic.stock-data}")
    // public void writeStockData(byte[] bytes) {
    //     try {
    //         StockData stockData = StockData.parseFrom(bytes);
    //         writer.write(stockData);
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }
}