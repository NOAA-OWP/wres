@0xb2997f6fcc49e9ec;

using Java = import "capnpJava/java.capnp";
using DurationDiagramMetric = import "durationdiagrammetric.capnp".DurationDiagramMetric;
using Duration = import "duration.capnp".Duration;
using Timestamp = import "timestamp.capnp".Timestamp;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("DurationDiagramStatisticOuter");

struct DurationDiagramStatistic
{
    # A message that encapsulates a duration diagram statistic, which contains a 
    # sequence of durations, each one referenced to an instant.

    metric @0 :DurationDiagramMetric;
    # This could be promoted to an Evaluation; however, there is one 
    # DiagramMetric per DiagramStatistic while the atomic collection is a pool, so 
    # there is no efficiency to be gained and some context to be lost.*/
    
    statistics @1 :List(PairOfInstantAndDuration);
    # The pairs of instants and durations.

    struct PairOfInstantAndDuration 
    {
        # A pair of one instant and one duration
        time @0 :Timestamp;
        duration @1 :Duration;
    }
}