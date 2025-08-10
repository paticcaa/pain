#pragma once

#define TOKENPASTE(x, y) x##y
#define TOKENPASTE2(x, y) TOKENPASTE(x, y)

// please refer to
// https://stackoverflow.com/questions/4295890/trouble-with-template-parameters-used-in-macros
#define EMPTY()
#define DEFER(...) __VA_ARGS__ EMPTY()

#define __RUN(statement)                                                                                               \
    static struct TOKENPASTE2(__run, __LINE__) {                                                                       \
        TOKENPASTE2(__run, __LINE__)                                                                                   \
        () {                                                                                                           \
            statement;                                                                                                 \
        }                                                                                                              \
    } TOKENPASTE2(TOKENPASTE2(__run, __LINE__), _instance);

#define RUN(...) __RUN(DEFER(__VA_ARGS__))
#define EXECUTE(...) __RUN(DEFER(__VA_ARGS__))

#define __REGISTER(cmd, parent, callback)                                                                              \
    static argparse::ArgumentParser cmd##_parser(format_command(#cmd));                                                \
    RUN(parent.add_subparser(cmd##_parser); cmd##_parser.add_parents(parent); callback(cmd##_parser);)
#define REGISTER(cmd, parent, ...) __REGISTER(cmd, parent, DEFER(__VA_ARGS__))

#define ARGS(cmd) cmd##_parser

#define COMMAND(name)                                                                                                  \
    Status name(argparse::ArgumentParser&);                                                                            \
    static struct __add_##name {                                                                                       \
        __add_##name() {                                                                                               \
            add(#name, name);                                                                                          \
        }                                                                                                              \
    } __add_##name##_instance;                                                                                         \
    Status name(argparse::ArgumentParser& args)

#define SPAN_1_ARGS(span)                                                                                              \
    auto tracer = pain::get_tracer("sad");                                                                             \
    auto span = tracer->StartSpan(__func__);                                                                           \
    auto scope = tracer->WithActiveSpan(span);                                                                         \
    span->SetAttribute("signature", __PRETTY_FUNCTION__);

#define SPAN_2_ARGS(span, name)                                                                                        \
    auto tracer = pain::get_tracer("sad");                                                                             \
    auto span = tracer->StartSpan(name);                                                                               \
    auto scope = tracer->WithActiveSpan(span);

#define GET_3TH_ARG(arg1, arg2, arg3, ...) arg3

#define SPAN_MACRO_CHOOSER(...) GET_3TH_ARG(__VA_ARGS__, SPAN_2_ARGS, SPAN_1_ARGS, )

#define SPAN(...) SPAN_MACRO_CHOOSER(__VA_ARGS__)(__VA_ARGS__)
