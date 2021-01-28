package com.ql.kafka.java;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: wautumnli
 * @date: 2021-01-28 14:47
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private Integer id;
    private String orderType;
}
