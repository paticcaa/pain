#pragma once

#include <gflags/gflags.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/ext/http/client/http_client.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_context.h>
#include <opentelemetry/sdk/trace/tracer_context_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/semantic_conventions.h>

#include <brpc/controller.h>
#include <string>

DECLARE_string(base_tracer_otlp_http_exporter_url);
DECLARE_bool(base_tracer_otlp_http_exporter_enable);
DECLARE_bool(base_tracer_otlp_file_exporter_enable);
DECLARE_string(base_tracer_otlp_file_exporter_path);

namespace pain {
class TraceLogHandle
    : public opentelemetry::sdk::common::internal_log::LogHandler {
    virtual void
    Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
           const char *file,
           int line,
           const char *msg,
           const opentelemetry::sdk::common::AttributeMap &) noexcept override;
};

inline std::shared_ptr<opentelemetry::trace::Tracer>
get_tracer(const std::string &tracer_name) {
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer(tracer_name);
}

inline std::string
get_trace_id(const std::shared_ptr<opentelemetry::trace::Span> &span) {
    const int TRACE_ID_LEN = 32;
    opentelemetry::trace::SpanContext span_context = span->GetContext();
    char trace_id_buf[TRACE_ID_LEN] = {};
    span_context.trace_id().ToLowerBase16(trace_id_buf);
    std::string trace_id = std::string(trace_id_buf, TRACE_ID_LEN);
    return trace_id;
}

inline std::string get_current_trace_id() {
    return get_trace_id(opentelemetry::trace::Tracer::GetCurrentSpan());
}

void init_tracer(const std::string &service_name);
void cleanup_tracer();
void inject_tracer(brpc::Controller *cntl);
opentelemetry::context::Context extract_context(brpc::Controller *cntl);
using Tracer = opentelemetry::trace::Tracer;
} // namespace pain
