@0xe720865573c47ce3;

using Java = import "java.capnp";
using MetricName = import "metricname.capnp".MetricName;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ScoreMetricOuter");

struct ScoreMetric
{
    # A message that encapsulates a score metric or scoring rule, which is composed 
    # of one or more scalar values.

    name @0 :MetricName;
    # Metric name.
    
    components @1 :List(ScoreMetricComponent);
    # One or more score components.
    
    struct ScoreMetricComponent
    {
        # A score component.
    
        enum ScoreComponentName 
        {
            # The name of a score component.

            unknown @0;
            mainScore @1;
            reliability @2; # Type-I bias
            resolution @3;
            uncertainty @4;
            typeIIBias @5;
            discrimination @6;
            sharpness @7;
            potential @8; 
        }

        name @0 :ScoreComponentName;

        units @1 :Text;
        # Units of the score.

        optimum @2 :Float64; 
        # The optimum value of the score (e.g., 1.0 for skill).

        minimum @3 :Float64; 
        # The lower bound of the score (e.g., 0 for the mean absolute error)

        maximum @4 :Float64; 
        # The upper bound of the score (e.g., 1 for pearson's correlation)
    }
}