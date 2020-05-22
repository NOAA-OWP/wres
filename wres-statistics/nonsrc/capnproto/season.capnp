@0xcb9de1056dcaea8d;

using Java = import "capnpJava/java.capnp";
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("SeasonOuter");

struct Season
{
    # A message that encapsulates a season, which is represented by a start 
    # monthday and an end monthday, both inclusive.

    startMonth @0 :UInt8;
    # Month number, 1-12.

    startDay @1 :UInt8;
    # Day number, 1-31.

    endMonth @2 :UInt8;
    # Month number, 1-12.

    endDay @3 :UInt8;
    # Day number, 1-12.
}