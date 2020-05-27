@0xd2f80f3d29b056cf;

using Java = import "java.capnp";
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ValueFilterOuter");

struct ValueFilter
{
    # A message that represents a filter applied to (all sides of) the pairs 
    # associated within an evaluation, prior to calculating statistics.

    minimumInclusiveValue @0 :Float64;
    # The smallest value that was considered, in evaluation measurement units.
    
    maximumInclusiveValue @1 :Float64;
    # The largest value that was considered, in evaluation measurement units.
}