package com.ql.flink.tableapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author： wanqiuli
 * DateTime: 2021/1/29 18:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

    private String id;
    private String name;
    private String dept;
}
