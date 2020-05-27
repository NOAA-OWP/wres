@0x98c6fa0d8d375d15;

using Java = import "java.capnp";
using Geometry = import "geometry.capnp".Geometry;
using TimeWindow = import "timewindow.capnp".TimeWindow;
using Pairs = import "pairs.capnp".Pairs;

$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("PoolOuter");

struct Pool
{
    # A message that encapsulates a pool, which is the atomic collection of pairs 
    # and statistics.

    geometries @0 :List(Geometry);
    # Zero or more geometries that characterize this pool.
    
    timeWindow @1 :TimeWindow;
    # The temporal boundaries of the pool.
    
    pairs @2 :Pairs;
    # The paired values that form the pool.
    
    baselinePairs @3 :Pairs;
    # The pairs for a baseline, where applicable.
}