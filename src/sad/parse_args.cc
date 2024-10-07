#include <argparse/argparse.hpp>

namespace pain::sad {

argparse::ArgumentParser& program() {
    static argparse::ArgumentParser s_program("program_name");
    return s_program;
}

// clang-format off
void init(int argc, char *argv[]) {
  program().add_argument("--log-level")
      .default_value(std::string("debug"))
      .required()
      .help("specify the log level");

  try {
    program().parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << err.what() << std::endl;
    std::cerr << program();
    std::exit(1);
  }
}
// clang-format off

} // namespace pain::sad
