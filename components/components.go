package components

import (
	"github.com/SigNoz/signoz-otel-collector/connectors/signozmeterconnector"
	"github.com/SigNoz/signoz-otel-collector/exporter/clickhouselogsexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/clickhousetracesexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/metadataexporter"
	"github.com/SigNoz/signoz-otel-collector/exporter/signozclickhousemeter"
	"github.com/SigNoz/signoz-otel-collector/exporter/signozclickhousemetrics"
	_ "github.com/SigNoz/signoz-otel-collector/pkg/parser/grok"
	"github.com/SigNoz/signoz-otel-collector/processor/signozlogspipelineprocessor"
	"github.com/SigNoz/signoz-otel-collector/processor/signozspanmetricsprocessor"
	"github.com/SigNoz/signoz-otel-collector/processor/signoztailsampler"
	"github.com/SigNoz/signoz-otel-collector/receiver/clickhousesystemtablesreceiver"
	"github.com/SigNoz/signoz-otel-collector/receiver/httplogreceiver"
	"github.com/SigNoz/signoz-otel-collector/receiver/signozawsfirehosereceiver"
	"github.com/SigNoz/signoz-otel-collector/receiver/signozkafkareceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8seventsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kubeletstatsreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receivercreator"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
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
		hostmetricsreceiver.NewFactory(),
		httplogreceiver.NewFactory(),
		k8sclusterreceiver.NewFactory(),
		k8seventsreceiver.NewFactory(),
		kubeletstatsreceiver.NewFactory(),
		prometheusreceiver.NewFactory(),
		prometheusremotewritereceiver.NewFactory(),
		receivercreator.NewFactory(),
		signozkafkareceiver.NewFactory(),
		signozawsfirehosereceiver.NewFactory(),
	}

	exporters := []exporter.Factory{
		clickhouselogsexporter.NewFactory(),
		signozclickhousemetrics.NewFactory(),
		clickhousetracesexporter.NewFactory(),
		debugexporter.NewFactory(),
		metadataexporter.NewFactory(),
		nopexporter.NewFactory(),
		signozclickhousemeter.NewFactory(),
	}

	processors := []processor.Factory{
		filterprocessor.NewFactory(),
		resourcedetectionprocessor.NewFactory(),
		signozspanmetricsprocessor.NewFactory(),
		signoztailsampler.NewFactory(),
		signozlogspipelineprocessor.NewFactory(),
	}

	connectors := []connector.Factory{
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
		healthcheckextension.NewFactory(),
		pprofextension.NewFactory(),
		zpagesextension.NewFactory(),
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
