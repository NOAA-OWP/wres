@0xd054c94c3f3f8f82;

using Java = import "capnpJava/java.capnp";
using MetricName = import "metricname.capnp".MetricName;
using Duration = import "duration.capnp".Duration;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("DurationDiagramMetricOuter");

struct DurationDiagramMetric
{
    # A message that encapsulates a duration diagram metric, which contains pairs 
    # of instants and durations.

    name @0 :MetricName;    
    # Metric name
    
    optimum @1 :Duration;
    # Optimum value, where application (e.g., zero for errors).

    minimum @2 :Duration;
    # The minimum duration value
        
    maximum @3 :Duration;
    # The maximum duration value
}