#pragma once

#include "base/tracer.h"

#define DEFINE_SPAN(span, controller)                                                                                  \
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);                                               \
    opentelemetry::trace::StartSpanOptions span##_options;                                                             \
    span##_options.kind = opentelemetry::trace::SpanKind::kServer;                                                     \
    auto span##_new_context = extract_context(cntl);                                                                   \
    span##_options.parent = opentelemetry::trace::GetSpan(span##_new_context)->GetContext();                           \
    auto span = get_tracer("manusya")->StartSpan(                                                                      \
        __func__,                                                                                                      \
        {                                                                                                              \
            {opentelemetry::trace::SemanticConventions::kRpcSystem, "brpc"},                                           \
            {opentelemetry::trace::SemanticConventions::kRpcService, cntl->method()->service()->name()},               \
            {opentelemetry::trace::SemanticConventions::kRpcMethod, cntl->method()->name()},                           \
            {opentelemetry::trace::SemanticConventions::kClientAddress,                                                \
             butil::endpoint2str(cntl->remote_side()).c_str()},                                                        \
            {opentelemetry::trace::SemanticConventions::kServerAddress,                                                \
             butil::endpoint2str(cntl->local_side()).c_str()},                                                         \
            {"signature", __PRETTY_FUNCTION__},                                                                        \
        },                                                                                                             \
        span##_options);                                                                                               \
    auto span##_scope = Tracer::WithActiveSpan(span);

#define SPAN_1_ARGS(span)                                                                                              \
    auto tracer = pain::get_tracer("manusya");                                                                         \
    auto span = tracer->StartSpan(__func__);                                                                           \
    auto scope = tracer->WithActiveSpan(span);                                                                         \
    span->SetAttribute("signature", __PRETTY_FUNCTION__);

#define SPAN_2_ARGS(span, name)                                                                                        \
    auto tracer = pain::get_tracer("manusya");                                                                         \
    auto span = tracer->StartSpan(name);                                                                               \
    auto scope = tracer->WithActiveSpan(span);                                                                         \
    span->SetAttribute("signature", __PRETTY_FUNCTION__);

#define GET_3TH_ARG(arg1, arg2, arg3, ...) arg3

#define SPAN_MACRO_CHOOSER(...) GET_3TH_ARG(__VA_ARGS__, SPAN_2_ARGS, SPAN_1_ARGS, )

#define SPAN(...) SPAN_MACRO_CHOOSER(__VA_ARGS__)(__VA_ARGS__)
