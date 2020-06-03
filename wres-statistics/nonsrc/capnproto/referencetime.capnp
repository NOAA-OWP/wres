@0xfb5c45dfe583334e;

using Java = import "java.capnp";
using Timestamp = import "timestamp.capnp".Timestamp;
using ReferenceTimeType = import "referencetimetype.capnp".ReferenceTimeType;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ReferenceTimeOuter");

struct ReferenceTime
{
    # A message that encapsulates a reference time and associated type, such as an
    # issued time.

    referenceTime @0 :Timestamp;
    # The reference time.
    
    referenceTimeType @1 :ReferenceTimeType;
    # The typeof reference time.
}