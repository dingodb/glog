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

#include "logging_async.h"  // NOLINT
#include "utilities.h"

#include <iostream>

namespace google {

AsyncLogger::AsyncLogger(google::base::Logger* logger, uint32_t maxBufferSize)
    : logger_(logger),
      maxBufferSize_(maxBufferSize),
      appThreadsBlockedCount_(0),
      flushCount_(0),
      activeBuffer_(new Buffer()),
      flushBuffer_(new Buffer()),
      running_(false),
      current_(0) {}

AsyncLogger::~AsyncLogger() {
    if (running_) {
        Stop();
    }
}

// NOTE: this function only run once in child process
void AsyncLogger::Switch2Child(bool start) {
    // We change pid before write message to file,
    // and the logger will never create a new file when pid changed,
    // so we can write parent and child log messages to one file with O_APPEND flag.
    // see LogFileObject::Write()
    if (!PidHasChanged() || current_ == 1) {
        return;
    }

    // After fork(2) and in the child process we should:
    //   (1) reset all status for AyncLogger
    //   (2) restart AyncLogger if necessary
    std::call_once(switched_, [this, start]{
        appThreadsBlockedCount_ = 0;
        flushCount_ = 0;
        activeBuffer_.reset(new Buffer());
        flushBuffer_.reset(new Buffer());
        running_ = false;
        current_ = 1;
        logger_->Switch2Child();

        if (start) {
            Start();
        }
    });
}

void AsyncLogger::Start() {
    Switch2Child(false);
    if (running_) {
        return;
    }

    running_ = true;
    loggerThreads_[current_] = std::thread(&AsyncLogger::RunThread, this);
}

void AsyncLogger::Stop() {
    Switch2Child(false);
    if (running_) {
        {
            // 获取锁，等待Write或Flush结束
            // 1. 避免写入过程中Stop
            // 2. 避免flush过程中Stop
            std::lock_guard<std::mutex> lk(mutexs_[current_]);

            // 设置为false
            running_ = false;

            wakeFluchCVs_[current_].notify_one();
        }
        loggerThreads_[current_].join();
    }
}

void AsyncLogger::Write(bool force_flush,
                        time_t timestamp,
                        const char* message,
                        int len) {
    Switch2Child(true);
    {
        std::unique_lock<std::mutex> ulk(mutexs_[current_]);

        if (running_ == false) {
            return;
        }

        // 如果buffer满, 则会阻塞到这里
        // 等到后台线程换出buffer
        while (IsBufferFull(*activeBuffer_)) {
            // 记录前台线程被阻塞的次数，测试使用
            ++appThreadsBlockedCount_;
            freeBufferCVs_[current_].wait(ulk);
        }

        activeBuffer_->Add(force_flush, timestamp, message, len);
        wakeFluchCVs_[current_].notify_one();
    }

    // glog在打印FATAL日志时, 会依次FATAL->INFO写入多个日志文件，然后abort
    // 对INFO WARNING ERROR级别开启了异步功能
    // 所以要保证这条FATAL日志写入文件后，再abort
    if (len > 0 && message[0] == 'F') {
        Flush();
    }
}

uint32_t AsyncLogger::LogSize() {
    return logger_->LogSize();
}

void AsyncLogger::RunThread() {
    while (running_ || activeBuffer_->NeedsWriteOrFlush()) {
        {
            std::unique_lock<std::mutex> ulk(mutexs_[current_]);

            // running_ == true
            //    如果activeBuffer为空, 则会等待一段时间，避免无效的swap
            // running_ == false
            //    AsyncLogger已经停止，但是aciveBuffer中仍然可能有未写入文件的日志
            if (!activeBuffer_->NeedsWriteOrFlush() && running_) {
                wakeFluchCVs_[current_].wait_for(
                    ulk, std::chrono::seconds(FLAGS_logbufsecs));
            }

            // 交换buffer
            flushBuffer_.swap(activeBuffer_);

            // flushBuffer满, 前台线程在阻塞过程中, 调用notify_all唤醒
            if (IsBufferFull(*flushBuffer_)) {
                freeBufferCVs_[current_].notify_all();
            }
        }

        // 逐条写入日志文件
        for (const auto& msg : flushBuffer_->messages) {
            logger_->Write(false, msg.timestamp,
                           msg.message.data(),
                           msg.message.size());
        }

        // flush
        logger_->Flush();

        ++flushCount_;
        flushBuffer_->Clear();
        flushCompleteCVs_[current_].notify_all();
    }
}

bool AsyncLogger::IsBufferFull(const Buffer& buf) const {
    return buf.size > maxBufferSize_;
}

void AsyncLogger::Flush() {
    std::unique_lock<std::mutex> ulk(mutexs_[current_]);
    uint64_t expectFlushCount = flushCount_ + 2;

    // flush 两次, 确保两个buffer都进行了flush
    while (flushCount_ < expectFlushCount && running_) {
        activeBuffer_->flush = true;
        wakeFluchCVs_[current_].notify_one();
        flushCompleteCVs_[current_].wait(ulk);
    }
}

}   // namespace google
