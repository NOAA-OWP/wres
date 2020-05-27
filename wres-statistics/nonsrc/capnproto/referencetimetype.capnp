@0xf05aefa7ffe03de9;

using Java = import "java.capnp";
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ReferenceTimeTypeOuter");

enum ReferenceTimeType
{
    # Type of forecast reference time
    
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
    # The time at which a time-series was published or "issued"
}