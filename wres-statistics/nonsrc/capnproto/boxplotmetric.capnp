@0xac78b734ddf6e7ea;

using Java = import "capnpJava/java.capnp";
using MetricName = import "metricname.capnp".MetricName;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("BoxplotMetricOuter");

struct BoxplotMetric
{
    # A message that encapsulates a box plot metric.

    name @0 :MetricName;    
    # Metric name.
    
    quantiles @1 :List(Float64);
    # The quantiles at which the whiskers are defined
    
    enum LinkedValueType
    {
        # The type of value linked to each box.
    
        unknown @0;
        observedValue @1;
        forecastValue @2;
        ensembleMean @3;
    }

    linkedValueType @2 :LinkedValueType;
    # The type of value linked to each box. For example,
    # when plotting boxes by observed value, this is 
    # LinkedValueType.observedValue.
    
    units @3 :Text;
    # Units of the quantiles
    
    optimum @4 :Float64; 
    # Optimum value, where application (e.g., zero for errors)

    minimum @5 :Float64; 
    # The lower bound of the quantiles

    maximum @6 :Float64; 
    # The upper bound of the quantiles
}