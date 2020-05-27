@0xbe439be9137a2001;

using Java = import "java.capnp";
using MetricName = import "metricname.capnp".MetricName;
using BoxplotMetric = import "boxplotmetric.capnp".BoxplotMetric;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("BoxplotStatisticOuter");

struct BoxplotStatistic
{
    # A message that encapsulates a box plot statistic.

    metric @0 :BoxplotMetric;
    # This could be promoted to an Evaluation; however, there is one 
    # BoxplotMetric per BoxplotStatistic while the atomic collection is a pool, so 
    # there is no efficiency to be gained and some context to be lost.

    statistics @1 :List(Box);
    # The boxes. 

    struct Box 
    {
        # A box.
	linkedValue @0 : Float64;

        quantiles @1 :List(Float64);
        # The whiskers
    }
}