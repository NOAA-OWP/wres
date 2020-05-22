@0xb3404e3a49c9d54a;

using Java = import "capnpJava/java.capnp";
using Duration = import "duration.capnp".Duration;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("TimeScaleOuter");

struct TimeScale
{
    # Message that encapsulates the time-scale at which an evaluation was 
    # performed.

    enum TimeScaleFunction
    {
    # An enumeration of functions used to distribute the value over the period.

           unknown @0;
           mean @1;
           total @2;
           maximum @3;
           minimum @4;
    }

    function @0 :TimeScaleFunction;
    # The time-scale function.
    
    period @1 :Duration;
    # Period over which the value is distributed.
}