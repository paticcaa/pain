#pragma once

#include <pain/base/tracer.h>

#define DEFINE_SPAN(module, span, controller)                                                                          \
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);                                               \
    opentelemetry::trace::StartSpanOptions span##_options;                                                             \
    span##_options.kind = opentelemetry::trace::SpanKind::kServer;                                                     \
    auto span##_new_context = extract_context(cntl);                                                                   \
    span##_options.parent = opentelemetry::trace::GetSpan(span##_new_context)->GetContext();                           \
    auto span = get_tracer(module)->StartSpan(__func__, {}, {}, span##_options);                                       \
    auto span##_scope = Tracer::WithActiveSpan(span);

#define SPAN_1_ARGS(span)                                                                                              \
    auto span = tracer->StartSpan(__func__);                                                                           \
    auto scope = tracer->WithActiveSpan(span);                                                                         \
    span->SetAttribute("signature", __PRETTY_FUNCTION__);

#define SPAN_2_ARGS(span, name)                                                                                        \
    auto span = tracer->StartSpan(name);                                                                               \
    auto scope = tracer->WithActiveSpan(span);                                                                         \
    span->SetAttribute("signature", __PRETTY_FUNCTION__);

#define GET_3TH_ARG(arg1, arg2, arg3, ...) arg3

#define SPAN_MACRO_CHOOSER(...) GET_3TH_ARG(__VA_ARGS__, SPAN_2_ARGS, SPAN_1_ARGS, )

#define SPAN(module, ...)                                                                                              \
    auto tracer = pain::get_tracer(module);                                                                            \
    SPAN_MACRO_CHOOSER(__VA_ARGS__)(__VA_ARGS__)
