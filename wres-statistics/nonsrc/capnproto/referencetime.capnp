@0xfb5c45dfe583334e;

using Java = import "capnpJava/java.capnp";
using Timestamp = import "timestamp.capnp".Timestamp;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ReferenceTimeOuter");

struct ReferenceTime
{
    # A message that encapsulates a reference time and associated type, such as an
    # issued time.

    enum ReferenceTimeType
    {
        # Type of forecast reference time.
      
        unknown @0;
        # An unknown reference time type.  
    
        t0 @1;
        # The time at which a model begins forward integration into a 
        # forecasting horizon, a.k.a. a forecast initialization time.
        
        analysisStartTime @2;
        # The start time of an analysis and assimilation period. The model 
        # begins forward integration at this time and continues until the forecast 
        # initialization time or T0.
        
        issuedTime @3;
        # The time at which a time-series was published or "issued".
    }

    referenceTime @0 :Timestamp;
    # The reference time.
    
    referenceTimeType @1 :ReferenceTimeType;
    # The typeof reference time.
}