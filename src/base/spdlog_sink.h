#include <butil/logging.h>
#include <spdlog/spdlog.h>

namespace pain::base {
class SpdlogSink : public logging::LogSink {
public:
    /*
      const LogSeverity BLOG_INFO = 0;
      const LogSeverity BLOG_NOTICE = 1;
      const LogSeverity BLOG_WARNING = 2;
      const LogSeverity BLOG_ERROR = 3;
      const LogSeverity BLOG_FATAL = 4;
      spdlog:
      enum level_enum : int
      {
          trace = SPDLOG_LEVEL_TRACE,
          debug = SPDLOG_LEVEL_DEBUG,
          info = SPDLOG_LEVEL_INFO,
          warn = SPDLOG_LEVEL_WARN,
          err = SPDLOG_LEVEL_ERROR,
          critical = SPDLOG_LEVEL_CRITICAL,
          off = SPDLOG_LEVEL_OFF,
          n_levels
      };
    */
    // Called when a log is ready to be written out.
    // Returns true to stop further processing.
    const spdlog::level::level_enum severity_to_level[5] = {
        spdlog::level::debug,
        spdlog::level::info,
        spdlog::level::warn,
        spdlog::level::err,
        spdlog::level::critical};

    virtual bool OnLogMessage(int severity, const char *file, int line, const butil::StringPiece &log_content) override {
        if (severity < 0) [[unlikely]] {
            severity = 0;
        }
        spdlog::level::level_enum level = severity_to_level[severity];
        std::string_view log_content_view(log_content.data(), log_content.size());
        spdlog::default_logger_raw()->log(spdlog::source_loc{file, line, nullptr},
                                          level,
                                          "{}",
                                          log_content_view);
        if (severity == logging::BLOG_FATAL) {
            spdlog::default_logger_raw()->flush();
        }
        return true;
    }
};

} // namespace pain::base