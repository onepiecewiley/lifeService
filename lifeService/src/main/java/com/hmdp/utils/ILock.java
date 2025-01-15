package com.hmdp.utils;

/**
 * @author onepiecewiley
 * @version 1.0
 * @date 2024/11/18 12:39
 */
public interface ILock {

    boolean tryLock(long timeoutSec);

    void unlock();
}
