#pragma once

#include <cstdlib>
#include <fmt/format.h>
#include <boost/assert.hpp>

namespace pain {

// path is in-out parameter
// path is like /tmp/test_pain_XXXXXX
// after calling this function, path will be the full path of the created directory
inline void make_temp_dir_or_die(std::string* path) {
    if (mkdtemp(path->data()) == nullptr) {
        BOOST_ASSERT_MSG(false, fmt::format("Failed to create temp directory: {}", strerror(errno)).c_str());
    }
}

// path is in-out parameter
// path is like /tmp/test_pain_XXXXXX
// after calling this function, path will be the full path of the created file
inline void make_temp_file_or_die(std::string* path) {
    if (mkstemp(path->data()) == -1) {
        BOOST_ASSERT_MSG(false, fmt::format("Failed to create temp file: {}", strerror(errno)).c_str());
    }
}

} // namespace pain
