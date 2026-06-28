package components

import (
	"github.com/SigNoz/signoz-otel-collector/connectors/signozmeterconnector"
	"github.com/SigNoz/signoz-otel-collector/exporter/clickhouselogsexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/clickhousetracesexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/jsontypeexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/metadataexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/signozclickhousemeter"
	"github.com/SigNoz/signoz-otel-collector/exporter/signozclickhousemetrics"
	signozhealthcheckextension "github.com/SigNoz/signoz-otel-collector/extension/healthcheckextension"
	_ "github.com/SigNoz/signoz-otel-collector/pkg/parser/grok"
	"github.com/SigNoz/signoz-otel-collector/processor/signozlogspipelineprocessor"
	"github.com/SigNoz/signoz-otel-collector/processor/signozspanmetricsprocessor"
	"github.com/SigNoz/signoz-otel-collector/processor/signoztailsampler"
	"github.com/SigNoz/signoz-otel-collector/processor/signoztransformprocessor"
	"github.com/SigNoz/signoz-otel-collector/receiver/clickhousesystemtablesreceiver"
	"github.com/SigNoz/signoz-otel-collector/receiver/httplogreceiver"
	"github.com/SigNoz/signoz-otel-collector/receiver/signozawsfirehosereceiver"
	"github.com/SigNoz/signoz-otel-collector/receiver/signozkafkareceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/countconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/metricsaslogsconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/otlpjsonconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/roundrobinconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/sumconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/azureauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/jaegerremotesampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oauth2clientauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/dockerobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/ecsobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/ecstaskobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/hostobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/k8sobserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oidcauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatorateprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/dnslookupprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/geoipprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbyattrsprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbytraceprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logdedupprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstransformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricsgenerationprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstarttimeprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/redactionprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dockerstatsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/jmxreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8seventsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sobjectsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kubeletstatsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/nsxtreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/otlpjsonfilereceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/podmanreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receivercreator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/simpleprometheusreceiver"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/forwardconnector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/zpagesextension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/nopreceiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"go.uber.org/multierr"
)

func Components() (otelcol.Factories, error) {
	extensions := []extension.Factory{}

	receivers := []receiver.Factory{
		clickhousesystemtablesreceiver.NewFactory(),
		dockerstatsreceiver.NewFactory(),
		hostmetricsreceiver.NewFactory(),
		httplogreceiver.NewFactory(),
		jmxreceiver.NewFactory(),
		k8sclusterreceiver.NewFactory(),
		k8seventsreceiver.NewFactory(),
		k8sobjectsreceiver.NewFactory(),
		kubeletstatsreceiver.NewFactory(),
		nsxtreceiver.NewFactory(),
		otlpjsonfilereceiver.NewFactory(),
		podmanreceiver.NewFactory(),
		prometheusreceiver.NewFactory(),
		prometheusremotewritereceiver.NewFactory(),
		receivercreator.NewFactory(),
		simpleprometheusreceiver.NewFactory(),
		signozkafkareceiver.NewFactory(),
		signozawsfirehosereceiver.NewFactory(),
	}

	exporters := []exporter.Factory{
		clickhouselogsexporter.NewFactory(),
		signozclickhousemetrics.NewFactory(),
		clickhousetracesexporter.NewFactory(),
		debugexporter.NewFactory(),
		jsontypeexporter.NewFactory(),
		metadataexporter.NewFactory(),
		nopexporter.NewFactory(),
		signozclickhousemeter.NewFactory(),
	}

	processors := []processor.Factory{
		attributesprocessor.NewFactory(),
		cumulativetodeltaprocessor.NewFactory(),
		deltatorateprocessor.NewFactory(),
		deltatocumulativeprocessor.NewFactory(),
		dnslookupprocessor.NewFactory(),
		filterprocessor.NewFactory(),
		geoipprocessor.NewFactory(),
		groupbyattrsprocessor.NewFactory(),
		groupbytraceprocessor.NewFactory(),
		k8sattributesprocessor.NewFactory(),
		logdedupprocessor.NewFactory(),
		logstransformprocessor.NewFactory(),
		metricsgenerationprocessor.NewFactory(),
		metricstransformprocessor.NewFactory(),
		metricstarttimeprocessor.NewFactory(),
		probabilisticsamplerprocessor.NewFactory(),
		redactionprocessor.NewFactory(),
		resourcedetectionprocessor.NewFactory(),
		resourceprocessor.NewFactory(),
		signozspanmetricsprocessor.NewFactory(),
		spanprocessor.NewFactory(),
		tailsamplingprocessor.NewFactory(),
		transformprocessor.NewFactory(),
		signoztailsampler.NewFactory(),
		signoztransformprocessor.NewFactory(),
		signozlogspipelineprocessor.NewFactory(),
	}

	connectors := []connector.Factory{
		failoverconnector.NewFactory(),
		countconnector.NewFactory(),
		forwardconnector.NewFactory(),
		metricsaslogsconnector.NewFactory(),
		otlpjsonconnector.NewFactory(),
		roundrobinconnector.NewFactory(),
		routingconnector.NewFactory(),
		servicegraphconnector.NewFactory(),
		signaltometricsconnector.NewFactory(),
		spanmetricsconnector.NewFactory(),
		sumconnector.NewFactory(),
		signozmeterconnector.NewFactory(),
	}

	factories, err := CoreComponents(
		extensions,
		receivers,
		processors,
		exporters,
		connectors,
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	return factories, err
}

func CoreComponents(
	extensions []extension.Factory,
	receivers []receiver.Factory,
	processors []processor.Factory,
	exporters []exporter.Factory,
	connectors []connector.Factory,
) (
	otelcol.Factories,
	error,
) {
	var errs []error

	extensions = append(
		extensions,
		basicauthextension.NewFactory(),
		bearertokenauthextension.NewFactory(),
		dockerobserver.NewFactory(),
		ecsobserver.NewFactory(),
		ecstaskobserver.NewFactory(),
		filestorage.NewFactory(),
		hostobserver.NewFactory(),
		jaegerremotesampling.NewFactory(),
		k8sobserver.NewFactory(),
		oauth2clientauthextension.NewFactory(),
		oidcauthextension.NewFactory(),
		healthcheckextension.NewFactory(),
		pprofextension.NewFactory(),
		signozhealthcheckextension.NewFactory(),
		zpagesextension.NewFactory(),
		azureauthextension.NewFactory(),
	)
	extensionsMap, err := otelcol.MakeFactoryMap(extensions...)
	if err != nil {
		errs = append(errs, err)
	}

	receivers = append(
		receivers,
		otlpreceiver.NewFactory(),
		nopreceiver.NewFactory(),
	)
	receiversMap, err := otelcol.MakeFactoryMap(receivers...)
	if err != nil {
		errs = append(errs, err)
	}

	exporters = append(
		exporters,
		otlpexporter.NewFactory(),
		otlphttpexporter.NewFactory(),
	)
	exportersMap, err := otelcol.MakeFactoryMap(exporters...)
	if err != nil {
		errs = append(errs, err)
	}

	processors = append(
		processors,
		batchprocessor.NewFactory(),
		memorylimiterprocessor.NewFactory(),
	)

	processorsMap, err := otelcol.MakeFactoryMap(processors...)
	if err != nil {
		errs = append(errs, err)
	}

	connectorsMap, err := otelcol.MakeFactoryMap(connectors...)
	if err != nil {
		errs = append(errs, err)
	}

	factories := otelcol.Factories{
		Extensions: extensionsMap,
		Receivers:  receiversMap,
		Processors: processorsMap,
		Exporters:  exportersMap,
		Connectors: connectorsMap,
		Telemetry:  otelconftelemetry.NewFactory(),
	}

	return factories, multierr.Combine(errs...)
}
