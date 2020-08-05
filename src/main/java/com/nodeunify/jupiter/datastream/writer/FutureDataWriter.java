package com.nodeunify.jupiter.datastream.writer;

import java.io.IOException;
import java.time.LocalDate;
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
import org.apache.parquet.proto.ProtoParquetWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class FutureDataWriter {

    @Value("${app.parquet.dir-path.future-data}")
    private String dirPath;
    @Value("${app.writer.future-data.instruments:}#{T(java.util.Collections).emptyList()}")
    private List<String> instruments;
    @Value("${app.writer.future-data.fields:}#{T(java.util.Collections).emptyList()}")
    private List<String> fields;

    private List<Pattern> patterns;
    private Path path;
    private ProtoParquetWriter<FutureData> writer;

    @PostConstruct
    public void postConstruct() {
        patterns = instruments.stream().map(instrument -> Pattern.compile(instrument, Pattern.CASE_INSENSITIVE))
                .collect(Collectors.toList());
        LocalDate now = LocalDate.now();
        String filePath = dirPath + 
            "type=data/" + 
            "year=" + now.getYear() + "/" + 
            "month=" + now.getMonthValue() + "/" + 
            now.format(DateTimeFormatter.ofPattern("YYYYMMDD")) + ".parquet";
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
                log.trace("futureData: {}", futureData.getCode());
                writer.write(futureData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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