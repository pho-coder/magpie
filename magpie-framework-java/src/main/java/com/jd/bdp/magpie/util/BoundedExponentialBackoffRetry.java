package com.jd.bdp.magpie.util;

import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 14-2-18
 * Time: 下午1:52
 * To change this template use File | Settings | File Templates.
 */

public class BoundedExponentialBackoffRetry extends ExponentialBackoffRetry {

    protected final int maxRetryInterval;

    public BoundedExponentialBackoffRetry(int baseSleepTimeMs,
                                          int maxRetries, int maxSleepTimeMs) {
        super(baseSleepTimeMs, maxRetries);
        this.maxRetryInterval = maxSleepTimeMs;
    }

    public int getMaxRetryInterval() {
        return this.maxRetryInterval;
    }

    @Override
    public int getSleepTimeMs(int count, long elapsedMs)
    {
        return Math.min(maxRetryInterval,
                super.getSleepTimeMs(count, elapsedMs));
    }
}
