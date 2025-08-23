#include <pain/base/brpc_text_map_carrier.h>
#include <pain/base/bthread_local_context_storage.h>
#include <pain/base/tracer.h>
#include <fstream>
#include <spdlog/spdlog.h>

DEFINE_string(base_tracer_otlp_http_exporter_url, "http://localhost:4318/v1/traces", "OTLP exporter URL");
DEFINE_bool(base_tracer_otlp_http_exporter_enable, true, "Enable OTLP HTTP exporter");
DEFINE_bool(base_tracer_otlp_file_exporter_enable, false, "Enable OTLP file exporter");
DEFINE_string(base_tracer_otlp_file_exporter_path, "trace_exporter", "OTLP file exporter path");
namespace otlp = opentelemetry::exporter::otlp;

namespace pain {

void TraceLogHandle::Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
                            const char* file,
                            int line,
                            const char* msg,
                            const opentelemetry::sdk::common::AttributeMap&) noexcept {
    const spdlog::level::level_enum levels[5] = {
        spdlog::level::debug, spdlog::level::err, spdlog::level::warn, spdlog::level::debug, spdlog::level::trace};

    auto l = static_cast<int>(level);
    spdlog::default_logger_raw()->log(spdlog::source_loc{file, line, ""}, levels[l], "{}", msg);
}

void init_tracer(const std::string& service_name) {
    std::shared_ptr<opentelemetry::sdk::common::internal_log::LogHandler> log_handler(new TraceLogHandle());
    opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogHandler(log_handler);
    opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogLevel(
        opentelemetry::sdk::common::internal_log::LogLevel::Debug);
    std::shared_ptr<opentelemetry::context::RuntimeContextStorage> storage(new BthreadLocalContextStorage());
    opentelemetry::context::RuntimeContext::SetRuntimeContextStorage(storage);

    std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>> processors;
    if (FLAGS_base_tracer_otlp_http_exporter_enable) {
        otlp::OtlpHttpExporterOptions opts;
        opts.url = FLAGS_base_tracer_otlp_http_exporter_url;
        auto exporter = otlp::OtlpHttpExporterFactory::Create(opts);

        auto processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
        processors.push_back(std::move(processor));
    }

    if (FLAGS_base_tracer_otlp_file_exporter_enable) {
        std::string filename = FLAGS_base_tracer_otlp_file_exporter_path + "." + service_name + ".otlp";
        static std::ofstream s_out(filename, std::ios::binary | std::ios::app);
        auto exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create(s_out);
        auto processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
        processors.push_back(std::move(processor));
    }

    auto resource = opentelemetry::sdk::resource::Resource::Create({{"service.name", service_name}});
    // Default is an always-on sampler.
    std::unique_ptr<opentelemetry::sdk::trace::TracerContext> context =
        opentelemetry::sdk::trace::TracerContextFactory::Create(std::move(processors), resource);

    std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
        opentelemetry::sdk::trace::TracerProviderFactory::Create(std::move(context));
    // Set the global trace provider
    opentelemetry::trace::Provider::SetTracerProvider(provider);

    // set global propagator
    opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
        std::shared_ptr<opentelemetry::context::propagation::TextMapPropagator>(
            new opentelemetry::trace::propagation::HttpTraceContext()));
}

void cleanup_tracer() {
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    opentelemetry::trace::Provider::SetTracerProvider(none);

    std::shared_ptr<opentelemetry::context::RuntimeContextStorage> storage;
    opentelemetry::context::RuntimeContext::SetRuntimeContextStorage(storage);
}

void inject_tracer(brpc::Controller* cntl) {
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    BrpcTextMapCarrier carrier(cntl);
    auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    prop->Inject(carrier, current_ctx);
}

opentelemetry::context::Context extract_context(brpc::Controller* cntl) {
    BrpcTextMapCarrier carrier(cntl);
    auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto new_context = prop->Extract(carrier, current_ctx);
    return new_context;
}

} // namespace pain
