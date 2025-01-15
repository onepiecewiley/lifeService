package com.hmdp.entity;

import lombok.Data;

import java.util.List;

/**
 * @author onepiecewiley
 * @version 1.0
 * @date 2024/11/27 18:53
 */
@Data
public class ScrollResult {
    private List<?> list;
    private Long minTime;
    private Integer offset;
}