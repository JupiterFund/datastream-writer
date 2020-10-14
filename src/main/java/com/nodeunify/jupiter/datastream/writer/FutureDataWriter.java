package com.nodeunify.jupiter.datastream.writer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.nodeunify.jupiter.datastream.v1.FutureData;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.Strings;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class FutureDataWriter {

    private static final Logger dataTraceLogger = LoggerFactory.getLogger("DataTraceLogger");

    @Value("${app.parquet.file-path:#{null}}")
    private String filePath;
    @Value("${app.parquet.dir-path.future-data}")
    private String dirPath;
    @Value("${app.writer.future-data.instruments:}#{T(java.util.Collections).emptyList()}")
    private List<String> instruments;
    @Value("${app.writer.future-data.fields:}#{T(java.util.Collections).emptyList()}")
    private List<String> fields;

    @Autowired
    AppConfig appConfig;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private List<Pattern> patterns;
    private Path path;
    private ProtoParquetWriter<FutureData> writer;
    private boolean writable = false;

    @PostConstruct
    public void postConstruct() {
        patterns = instruments.stream().map(instrument -> Pattern.compile(instrument, Pattern.CASE_INSENSITIVE))
                .collect(Collectors.toList());
        if (Strings.isNullOrEmpty(filePath)) {
            // 凌晨后收数据，数据日期应为前一天
            // LocalDate yesterday = LocalDate.now().minusDays(1);
            // 日盘收盘后收数据，数据日期应为当天
            LocalDate today = LocalDate.now();
            filePath = dirPath + 
                "type=data/" + 
                "year=" + today.getYear() + "/" + 
                "month=" + today.getMonthValue() + "/" + 
                today.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + ".parquet";
        }
        log.debug("输出Parquet文件: {}", filePath);
        path = new Path(filePath);
        try {
            writer = new ProtoParquetWriter<FutureData>(path, 
                FutureData.class, CompressionCodecName.GZIP, 
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetProperties.DEFAULT_PAGE_SIZE);
            writable = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (writer != null && writable) {
            try {
                log.info("关闭ParquetWriter");
                writer.close();
                writable = false;
            } catch (IOException e) {
                log.error("Parquet文件关闭异常", e);
            }
        }
    }

    private void stopKafkaListener() {
        MessageListenerContainer listener = registry.getListenerContainer("futureDataListner");
        if (listener.isRunning()) {
            log.info("关闭数据监听");
            // KafkaListener正常后，程序会正常退出。preDestroy方法会被自动运行
            listener.stop();
        }
    }

    @KafkaListener(id="futureDataListner", idIsGroup=false, topics="${spring.kafka.topic.future-data}", containerFactory="kafkaListenerContainerFactory")
    public void writeFutureData(ConsumerRecord<String, byte[]> record) {
        LocalDateTime kafkaTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()),
                ZoneId.systemDefault());
        if (kafkaTime.isBefore(appConfig.getEndTime())) {
            try {
                FutureData futureData = FutureData.parseFrom(record.value());
                FutureData.Builder builder = futureData.toBuilder();
                // 暂时不使用。不确定无效值在数据使用过程中的重要性
                // sanitize(builder);
                final String code = builder.getCode();
                // 过滤期货合约或合约品种。支持正则式定义。
                boolean matches = patterns.size() == 0 ? 
                    true : 
                    patterns
                        .stream()
                        .filter(pattern -> pattern.matcher(code).matches())
                        .findAny()
                        .isPresent();
                if (matches) {
                    Descriptors.Descriptor descriptor = builder.getDescriptorForType();
                    // 过滤数据字段。支持包含和排除定义。
                    fields
                        .stream()
                        .filter(field -> field.startsWith("-"))
                        .forEach(field -> {
                            FieldDescriptor fd = descriptor.findFieldByName(field.substring(1));
                            if (fd != null) {
                                builder.clearField(fd);
                            }
                        });
                    futureData = builder.build();
                    dataTraceLogger.trace("Kafka FutureData", "FutureData", futureData.getCode(), futureData.getTime(), futureData.getDate(), futureData.getTradeDate());
                    if (writable) {
                        writer.write(futureData);
                    } else {
                        log.warn("ParquetWriter已关闭");
                        stopKafkaListener();
                    }
                }
            } catch (IOException e) {
                log.error("写入Parquet文件错误", e);
            } catch (Exception e) {
                log.error("数据落地异常", e);
            }
        } else {
            log.info("全部指定数据读取完毕");
            stopKafkaListener();
        }
    }

    @EventListener(condition = "event.listenerId.startsWith('futureDataListner-')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        log.warn("接收数据闲置超过限时");
        stopKafkaListener();
    }

    // 上游得到的数据中存在Long型最大值作为缺省无效值
    // 为了减少落地文件占用的容量，手动将所有缺省值转为0
    // private void sanitize(FutureData.Builder builder) {
    //     if (builder.getAuctionPrice() == Long.MAX_VALUE) {
    //         builder.clearAuctionPrice();
    //     }
    //     if (builder.getAuctionQty() == Long.MAX_VALUE) {
    //         builder.clearAuctionQty();
    //     }
    //     if (builder.getAvgPrice() == Integer.MAX_VALUE) {
    //         builder.clearAvgPrice();
    //     }
    //     if (builder.getClosePx() == Long.MAX_VALUE) {
    //         builder.clearClosePx();
    //     }
    //     if (builder.getCurrDelta() == Integer.MAX_VALUE) {
    //         builder.clearCurrDelta();
    //     }
    //     if (builder.getHighPx() == Long.MAX_VALUE) {
    //         builder.clearHighPx();
    //     }
    //     if (builder.getLastPx() == Long.MAX_VALUE) {
    //         builder.clearLastPx();
    //     }
    //     if (builder.getLowPx() == Long.MAX_VALUE) {
    //         builder.clearLowPx();
    //     }
    //     if (builder.getOpenInterest() == Long.MAX_VALUE) {
    //         builder.clearOpenInterest();
    //     }
    //     if (builder.getOpenPx() == Long.MAX_VALUE) {
    //         builder.clearOpenPx();
    //     }
    //     if (builder.getPreClosePx() == Long.MAX_VALUE) {
    //         builder.clearPreClosePx();
    //     }
    //     if (builder.getPreDelta() == Integer.MAX_VALUE) {
    //         builder.clearPreDelta();
    //     }
    //     if (builder.getPreOpenInterest() == Long.MAX_VALUE) {
    //         builder.clearPreOpenInterest();
    //     }
    //     if (builder.getPreSettlePrice() == Long.MAX_VALUE) {
    //         builder.clearPreSettlePrice();
    //     }
    //     if (builder.getPriceDownLimit() == Long.MAX_VALUE) {
    //         builder.clearPriceDownLimit();
    //     }
    //     if (builder.getPriceUpLimit() == Long.MAX_VALUE) {
    //         builder.clearPriceUpLimit();
    //     }
    //     if (builder.getSettlePrice() == Long.MAX_VALUE) {
    //         builder.clearSettlePrice();
    //     }
    //     if (builder.getTotalValueTrade() == Long.MAX_VALUE) {
    //         builder.clearTotalValueTrade();
    //     }
    //     if (builder.getTotalVolumeTrade() == Long.MAX_VALUE) {
    //         builder.clearTotalVolumeTrade();
    //     }
    //     List<Long> bidPriceList = builder.getBidPriceList();
    //     if (bidPriceList.removeIf(bidPrice -> bidPrice == Long.MAX_VALUE)) {
    //         builder.addAllBidPrice(bidPriceList);
    //     }
    //     List<Long> bidQtyList = builder.getBidQtyList();
    //     if (bidQtyList.removeIf(bidQty -> bidQty == Long.MAX_VALUE)) {
    //         builder.addAllBidQty(bidQtyList);
    //     }
    //     List<Long> offerPriceList = builder.getOfferPriceList();
    //     if (offerPriceList.removeIf(offerPrice -> offerPrice == Long.MAX_VALUE)) {
    //         builder.addAllOfferPrice(offerPriceList);
    //     }
    //     List<Long> offerQtyList = builder.getOfferQtyList();
    //     if (offerQtyList.removeIf(offerQty -> offerQty == Long.MAX_VALUE)) {
    //         builder.addAllOfferQty(offerQtyList);
    //     }
    // }
}