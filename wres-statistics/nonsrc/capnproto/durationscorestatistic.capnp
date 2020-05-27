@0xd997ba1f5198dca5;

using Java = import "java.capnp";
using Duration = import "duration.capnp".Duration;
using DurationScoreMetric = import "durationscoremetric.capnp".DurationScoreMetric;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("DurationScoreStatisticOuter");

struct DurationScoreStatistic
{
    # A message that encapsulates a score statistic, which is the product of 
    # applying a score metric to a pool of pairs.

    metric @0 :DurationScoreMetric;
    # This could be promoted to an Evaluation; however, there is one 
    # DurationScoreMetric per DurationScoreStatistic while the atomic collection 
    # is a pool, so there is no efficiency to be gained and some context to be 
    # lost.

    statistics @1 :List(DurationScoreStatisticComponent);
    # The duration score components. This could be abstracted with repeated 
    # durations and an implicit ordering based on the ordering in 
    # DurationScoreMetric instead.
    
    struct DurationScoreStatisticComponent
    {
        # A diagram component.
    
        name @0 :DurationScoreMetric.DurationScoreMetricComponent.DurationScoreComponentName;
        # Metric component name.
        
        value @1 :Duration;
        # Component value.
    }

}