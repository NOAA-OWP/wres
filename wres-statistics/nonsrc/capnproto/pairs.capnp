@0xb2bd5351cc1bf901;

using Java = import "capnpJava/java.capnp";
using Timestamp = import "timestamp.capnp".Timestamp;
using ReferenceTime = import "referencetime.capnp".ReferenceTime;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("PairsOuter");

struct Pairs
{
    # A message that encapsulated pairs, which must be continuous numerical 
    # (single-valued or ensemble). The time-series composition of the paired 
    # values is respected by the message.

    timeSeries @0 :List(TimeSeriesOfPairs);
    # Zero or more time-series of pairs.

    struct TimeSeriesOfPairs
    {
        # A time-series of paired values.

        referenceTimes @0 :List(ReferenceTime);
        # Zero or more reference times.

        pairs @1 :List(Pair);
        # One or more pairs.
    }

    struct Pair 
    {
        # A single pair composed of one or more left values and one or more right
        # values

        validTime @0 :Timestamp;
        # The valid time of the pair.

        left @1 :List(Float64);
        # One or more left values.

        right @2 :List(Float64);
        # One or more right values.
    }

}