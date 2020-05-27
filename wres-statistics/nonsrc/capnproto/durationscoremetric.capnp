@0xbd1b8237e33a7a7a;

using Java = import "java.capnp";
using MetricName = import "metricname.capnp".MetricName;
using Duration = import "duration.capnp".Duration;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("DurationScoreMetricOuter");

struct DurationScoreMetric
{
    # A message that encapsulates a score metric or scoring rule that produces a duration. */

    name @0 :MetricName;
    # Metric name.
    
    components @1 :List(DurationScoreMetricComponent);
    # One or more score components.

    struct DurationScoreMetricComponent
    {
        # A score component.
    
        enum DurationScoreComponentName 
        {           
            # A score component name.
    
            unknown @0; 
            mean @1;
            median @2;
            minimum @3;
            maximum @4;
            standardDeviation @5;
            meanAbsolute @6;
        }

        name @0 :DurationScoreComponentName;

        optimum @1 :Duration; 
        # The optimum value of the score (e.g., 0.0 for the mean error in timing)

        minimum @2 :Duration; 
        # The lower bound of the score

        maximum @3 :Duration; 
        # The upper bound of the score.
    }
}