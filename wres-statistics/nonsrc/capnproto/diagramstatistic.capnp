@0xb09a8f76db2c7eaf;

using Java = import "java.capnp";
using MetricName = import "metricname.capnp".MetricName;
using DiagramMetric = import "diagrammetric.capnp".DiagramMetric;
using Threshold = import "threshold.capnp".Threshold;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("DiagramStatisticOuter");

struct DiagramStatistic
{
    # A message that encapsulates a diagram statistic, such as the binned 
    # statistics associated with a reliability diagram.

    metric @0 :DiagramMetric;

    eventThreshold @1 :Threshold;
    # Threshold used to define a partition of pairs or categorical event.
    
    decisionThreshold @2 :Threshold;
    # Threshold used to convert a probability into a dichotomous outcome.
    
    statistics @3 :List(DiagramStatisticComponent);
    # The diagram component values.

    struct DiagramStatisticComponent 
    {
        # A diagram component

        name @0 :DiagramMetric.DiagramMetricComponent.DiagramComponentName;
        # Metric component name
        
        values @1 :List(Float64);
        # Component values.
        
    }
}