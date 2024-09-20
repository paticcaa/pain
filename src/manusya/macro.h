#pragma once

#define DEFINE_SPAN(span, controller)                                        \
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);     \
    opentelemetry::trace::StartSpanOptions span##_options;                   \
    span##_options.kind = opentelemetry::trace::SpanKind::kServer;           \
    auto span##_new_context = extract_context(cntl);                         \
    span##_options.parent =                                                  \
        opentelemetry::trace::GetSpan(span##_new_context)->GetContext();     \
    auto span = get_tracer("manusya")->StartSpan(                            \
        __func__,                                                            \
        {                                                                    \
            {opentelemetry::trace::SemanticConventions::kRpcSystem, "brpc"}, \
            {opentelemetry::trace::SemanticConventions::kRpcService,         \
             cntl->method()->service()->name()},                             \
            {opentelemetry::trace::SemanticConventions::kRpcMethod,          \
             cntl->method()->name()},                                        \
            {opentelemetry::trace::SemanticConventions::kClientAddress,      \
             butil::endpoint2str(cntl->remote_side()).c_str()},              \
            {opentelemetry::trace::SemanticConventions::kServerAddress,      \
             butil::endpoint2str(cntl->local_side()).c_str()},               \
        },                                                                   \
        span##_options);                                                     \
    auto span##_scope = Tracer::WithActiveSpan(span);
