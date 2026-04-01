#ifndef CYBER_LOGGER_HPP
#define CYBER_LOGGER_HPP

#include <iostream>
#include <iomanip>
#include <chrono>
#include <mutex>
#include "string.h"
#include "strings.h"

#define DEBUG

#ifdef DEBUG

#define CYBER_INFO(format, ...) \
    printf("[INFO] " format "\r\n", ##__VA_ARGS__);

#define CYBER_WARN(format, ...) \
    printf("[WARNING] [%s:%d] " format "\r\n", __FILE__, __LINE__, ##__VA_ARGS__);

#define CYBER_ERROR(format, ...) \
    printf("[ERROR] [%s:%d] " format "\r\n", __FILE__, __LINE__, ##__VA_ARGS__);

#endif

namespace cyber
{
    class logger
    {
    private:
    public:
        logger() = default;
        ~logger() = default;

        // static std::mutex mlock;

        static void systemlog(std::string &&s)
        {
            // std::lock_guard<std::mutex>(logger::mlock);
            auto t = std::chrono::high_resolution_clock::now();
            auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch());
            auto timestamp = nano.count() / 1E9;
            std::cout << "[SYS] " << std::setprecision(10) << std::fixed << timestamp << " [" << s << "] " << std::endl;
        }

        static void log(std::string &&s)
        {
            // std::lock_guard<std::mutex>(logger::mlock);
            auto t = std::chrono::high_resolution_clock::now();
            auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch());
            auto timestamp = nano.count() / 1E9;
            std::cout << "[INFO] " << std::setprecision(10) << std::fixed << timestamp << " [" << s << "] " << std::endl;
        }

        static void log(std::string &s)
        {
            logger::log(std::move(s));
        }
    };
}

#endif