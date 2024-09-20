#pragma once

#include <boost/preprocessor.hpp>

#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include "base/scope_exit.h"

// __PLOG_PREPROCESS_PAIRS
// __PLOG_PREPROCESS_PAIRS((a, 1)(b, 2)(c, 3)) -> ((a, 1))((b, 2))((c, 3))
#define __PLOG_ADD_PAREN_1(x, y) ((x, y)) __PLOG_ADD_PAREN_2
#define __PLOG_ADD_PAREN_2(x, y) ((x, y)) __PLOG_ADD_PAREN_1
#define __PLOG_ADD_PAREN_1_END
#define __PLOG_ADD_PAREN_2_END
#define __PLOG_PREPROCESS_PAIRS(pairs) \
    BOOST_PP_CAT(__PLOG_ADD_PAREN_1 pairs, _END)

#define __PLOG_PROCESS_KEY_ELEMENT(r, unused, idx, elem) \
    BOOST_PP_EXPR_IF(idx, " ")                           \
    BOOST_PP_TUPLE_ELEM(2, 0, elem)                      \
    ":{}"
#define __PLOG_PROCESS_VALUE_ELEMENT(r, unused, idx, elem) \
    BOOST_PP_COMMA_IF(idx)                                 \
    BOOST_PP_TUPLE_ELEM(2, 1, elem)

// dynamic debug support
#define GET_FIRST_VA_ARG(arg1, ...) arg1
#define FIRST_VA_ARG(...) GET_FIRST_VA_ARG(__VA_ARGS__)

#define __PLOG(LEVEL, seq)                                                                                        \
    do {                                                                                                          \
        constexpr const char *fmt = BOOST_PP_SEQ_FOR_EACH_I(                                                      \
            __PLOG_PROCESS_KEY_ELEMENT, % %, __PLOG_PREPROCESS_PAIRS(seq));                                       \
        SPDLOG_##LEVEL(fmt,                                                                                       \
                       BOOST_PP_SEQ_FOR_EACH_I(__PLOG_PROCESS_VALUE_ELEMENT, % %, __PLOG_PREPROCESS_PAIRS(seq))); \
    } while (0)

#define PLOG_TRACE(seq)                             \
    if (spdlog::should_log(spdlog::level::trace)) { \
        __PLOG(TRACE, seq);                         \
    }
#define PLOG_DEBUG(seq)                             \
    if (spdlog::should_log(spdlog::level::debug)) { \
        __PLOG(DEBUG, seq);                         \
    }
#define PLOG_INFO(seq)                             \
    if (spdlog::should_log(spdlog::level::info)) { \
        __PLOG(INFO, seq);                         \
    }
#define PLOG_WARN(seq)                             \
    if (spdlog::should_log(spdlog::level::warn)) { \
        __PLOG(WARN, seq);                         \
    }
#define PLOG_ERROR(seq)                           \
    if (spdlog::should_log(spdlog::level::err)) { \
        __PLOG(ERROR, seq);                       \
    }
#define PLOG_CRITICAL(seq)                             \
    if (spdlog::should_log(spdlog::level::critical)) { \
        __PLOG(CRITICAL, seq);                         \
    }

// clang-format off
#define PFMT(seq) \
    BOOST_PP_SEQ_FOR_EACH_I(__PLOG_PROCESS_KEY_ELEMENT, % %, __PLOG_PREPROCESS_PAIRS(seq)), BOOST_PP_SEQ_FOR_EACH_I(__PLOG_PROCESS_VALUE_ELEMENT, % %, __PLOG_PREPROCESS_PAIRS(seq))

// snake style
#define pfmt(seq) PFMT(seq)
// clang-format on

namespace pain {
// log_level: trace, debug, info, warn, err, critical
typedef spdlog::level::level_enum log_level;

struct LoggerOptions {
    std::string file_name;
    std::string name;
    std::size_t file_size = 100 << 20;
    std::chrono::seconds flush_period{2};
    log_level level_log = log_level::warn;

    // maximum number of log files
    size_t rotate = 10;

    // the size of thread queue
    size_t queue_size = 32 << 10;

    // number of log processing threads
    // the variable defaults to 1 to ensure chronological order
    size_t async_threads = 1;
};

// defining "log_tp" will start a thread for processing log printing
inline static auto make_logger(const LoggerOptions &logger_options) {
    auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        logger_options.file_name, logger_options.file_size, logger_options.rotate, false);
    spdlog::init_thread_pool(logger_options.queue_size,
                             logger_options.async_threads);
    auto log_tp = spdlog::thread_pool();
    auto logger = std::make_shared<spdlog::async_logger>(
        logger_options.name, rotating_sink, log_tp, spdlog::async_overflow_policy::block);

    spdlog::set_default_logger(logger);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%n] [%^%l%$] %s:%# %v");
    logger->set_level(logger_options.level_log);
    logger->flush_on(log_level::err);
    spdlog::flush_every(logger_options.flush_period);

    auto flush_log = make_scope_exit([logger, log_tp]() mutable {
        logger->flush();
        // keep logger tp alive until flush() completes
        log_tp.reset();
        spdlog::shutdown();
    });
    return flush_log;
}

} // namespace pain
