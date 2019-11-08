/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

public class TimeCache {
  // 静态变量，自己的对象实例
  private static TimeCache timeCacheInstance = new TimeCache();
  // 时间变量
  private long time;

  // 私有构造方法，限制外面拿到TimeCache的实例
  private TimeCache() {
    time = -1;
  }

  // 静态初始化实例
  public static TimeCache getTimeCacheInstance() {
    return timeCacheInstance;
  }

  public void updateTime(long newTime) {
    if (this.time == -1 || this.time < newTime) {
      this.time = newTime;
      System.out.printf("Time updated successes.Time : %d \n", this.time);
    } else {
      return;
    }
  }

  // 返回时间的方法
  public long time() {
    return time;
  }
}