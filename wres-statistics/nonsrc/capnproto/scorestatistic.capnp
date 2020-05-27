@0xc75b37e1da91ca8e;

using Java = import "java.capnp";
using ScoreMetric = import "scoremetric.capnp".ScoreMetric;
using Threshold = import "threshold.capnp".Threshold;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ScoreStatisticOuter");

struct ScoreStatistic
{
    # A message that encapsulates a score statistic, which is the product of 
    # applying a score metric to a pool of pairs.

    metric @0 :ScoreMetric;
    # This could be promoted to an Evaluation; however, there is one 
    # ScoreMetric per ScoreStatistic while the atomic collection is a pool, so 
    # there is no efficiency to be gained and some context to be lost.
    
    eventThreshold @1 :Threshold;
    decisionThreshold @2 :Threshold;

    statistics @3 :List(ScoreStatisticComponent);
    # The score components. This could be abstracted with repeated doubles and 
    # an implicit ordering based on the ordering in ScoreMetric instead

    struct ScoreStatisticComponent
    {
        name @0 :ScoreMetric.ScoreMetricComponent.ScoreComponentName;
        # Metric component name

        value @1 :Float64;
        # Score component value.
    }
}