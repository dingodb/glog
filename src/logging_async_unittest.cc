/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <atomic>
#include <thread>   // NOLINT
#include <vector>
#include <chrono>   // NOLINT

#include "glog/logging.h"
#include "logging_async.h"
#include "googletest.h"

class CountLogger : public google::base::Logger {
public:
  void Write(bool force_flush,
             time_t      /*timestamp*/,
             const char* /*message*/,
             int         /*message_len*/) override {
    message_count_++;
    if (force_flush) {
      Flush();
    }
  }

  void Flush() override {
    // Simulate a slow disk.
    std::this_thread::sleep_for(std::chrono::seconds(5));
    flush_count_++;
  }

  uint32_t LogSize() override {
    return 0;
  }

  std::atomic<int> flush_count_ = {0};
  std::atomic<int> message_count_ = {0};
};

int main(int argc, char* argv[]) {
  using google::AsyncLogger;

  const int kNumThreads = 4;
  const int kNumMessages = 10000;
  const int kBuffer = 10000;
  std::unique_ptr<CountLogger> logger(new CountLogger);

  AsyncLogger async(logger.get(), kBuffer);
  async.Start();

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      for (int m = 0; m < kNumMessages; ++m) {
        async.Write(true, m, "x", 1);
      }
    });
  }

  threads.emplace_back([&]() {
    for (int i = 0; i < 10; ++i) {
      async.Flush();
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
  });

  for (auto& t : threads) {
    t.join();
  }

  async.Stop();

  CHECK_EQ(logger->message_count_, kNumMessages * kNumThreads);
  CHECK_LT(logger->flush_count_, kNumMessages * kNumThreads);
  logger.release();  // 内存已由async接管
}
