#ifndef UTIL_LOGGING_H
#define UTIL_LOGGING_H

namespace dsm {
class Logger {
public:
  void debug(const char* fmt, ...);
  void info(const char* fmt, ...);
  void warn(const char* fmt, ...);
  void error(const char* fmt, ...);
  void fatal(const char* fmt, ...);
};

void log_debug(const char* fmt, ...);
void log_info(const char* fmt, ...);
void log_warn(const char* fmt, ...);
void log_error(const char* fmt, ...);
void log_fatal(const char* fmt, ...);
}

#endif
