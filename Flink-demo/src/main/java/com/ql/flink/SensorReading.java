package com.ql.flink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: wautumnli
 * @date: 2021-08-16 23:32
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String id;
    private Long timestamp;
    private Double temperature;
}
