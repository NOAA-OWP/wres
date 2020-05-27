@0xab0799dd2a2fc67c;

using Java = import "java.capnp";
using Timestamp = import "timestamp.capnp".Timestamp;
using Duration = import "duration.capnp".Duration;
using ReferenceTimeType = import "referencetimetype.capnp".ReferenceTimeType;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("TimeWindowOuter");

struct TimeWindow
{
    # A message that encapsulates the temporal boundaries of a pool.

    earliestReferenceTime @0 :Timestamp;
    latestReferenceTime @1 :Timestamp;
    earliestValidTime @2 :Timestamp;
    latestValidTime @3 :Timestamp;
    earliestLeadDuration @4 :Duration;
    latestLeadDuration @5 :Duration;

    referenceTimeType @6 :List(ReferenceTimeType);
    # The types of reference times to which the time window applies.
}