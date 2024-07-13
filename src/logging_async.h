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

#ifndef SRC_LOGGING_ASYNC_H_
#define SRC_LOGGING_ASYNC_H_

#include <string>
#include <vector>
#include <thread>   // NOLINT
#include <atomic>
#include <mutex>    // NOLINT
#include <condition_variable>   // NOLINT
#include <ctime>

#include <glog/logging.h>   // NOLINT

namespace google {

class AsyncLogger : public google::base::Logger {
public:
    AsyncLogger(google::base::Logger* logger, uint32_t maxBufferSize);
    virtual ~AsyncLogger();

    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    void Write(bool force_flush,
                       time_t timestamp,
                       const char* message,
                       int message_len) override;

    // 日志刷新到文件
    void Flush() override;
    uint32_t LogSize() override;

    void Start();
    void Stop();

private:
    // A buffered message
    struct Message {
        time_t timestamp;
        std::string message;

        Message(time_t ts, const char* msg, int len)
          : timestamp(ts), message(msg, len) {}
    };

    struct Buffer {
        std::vector<Message> messages;
        uint32_t size;
        bool flush;

        Buffer() : messages(), size(0), flush(false) {}

        void Clear() {
            messages.clear();
            size = 0;
            flush = false;
        }

        void Add(bool force_flush, time_t timestamp,
                 const char* message, int len) {
            if (!message) {
                return;
            }

            messages.emplace_back(timestamp, message, len);
            size += len;
            flush |= force_flush;
        }

        bool NeedsWriteOrFlush() const {
            return size != 0 || flush;
        }
    };

    bool IsBufferFull(const Buffer& buf) const;

    void RunThread();

    void Switch2Child(bool start);

    // 后台线程写日志文件的logger
    std::unique_ptr<google::base::Logger> logger_;

    // 后台写日志线程
    std::thread loggerThreads_[2];

    // buffer中日志的最大字节数
    const uint32_t maxBufferSize_;

    // buffer满时, 前台线程被阻塞的次数，for test
    std::atomic<uint32_t> appThreadsBlockedCount_;

    // flush的次数
    std::atomic<uint64_t> flushCount_;

    mutable std::mutex mutexs_[2];

    // 用于唤醒后台写日志线程
    std::condition_variable wakeFluchCVs_[2];

    // 用于唤醒前台线程
    std::condition_variable freeBufferCVs_[2];

    // 用于同步Flush过程
    std::condition_variable flushCompleteCVs_[2];

    // 前台线程将日志保存到active buffer中
    std::unique_ptr<Buffer> activeBuffer_;

    // 后台线程将flush buffer中的日志写入文件
    std::unique_ptr<Buffer> flushBuffer_;

    std::atomic<bool> running_;

    int current_;

    std::once_flag switched_;
};

}  // namespace google

#endif  // SRC_LOGGING_ASYNC_H_
