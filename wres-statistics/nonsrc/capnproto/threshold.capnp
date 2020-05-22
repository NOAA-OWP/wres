@0xcf6d98cdf57c90b7;

using Java = import "capnpJava/java.capnp";
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("ThresholdOuter");

struct Threshold
{
    # A message that encapsulates a threshold, which may contain one or two values
    # with prescribed units and a user-friendly name.

    type @0 :ThresholdType;
    # Type of threshold.

    name @1 :Text;
    # A user-friendly name for a threshold (e.g., MINOR FLOOD).

    leftThresholdValue @2 :Float64;
    # A real-valued left threshold.
    
    rightThresholdValue @3 :Float64;
    # A real-valued right threshold when the ThresholdType is BETWEEN.
    
    leftThresholdProbability @4 :Float64;
    # The probability associated with the left threshold.
    
    rightThresholdProbability @5 :Float64;
    # The probability associated with the right threshold when BETWEEN.
    
    thresholdValueUnits @6 :Text;
    # Threshold measurement units.
    
    enum ThresholdType 
    {
        # Identifies the type of threshold.        

        less @0;
        # Identifier for less than.

        greater @1;
        # Identifier for greater than.

        lessEqual @2;
        # Identifier for less than or equal to.

        greaterEqual @3;
        # Identifier for greater than or equal to.

        equal @4;
        # Identifier for equality.

        between @5;
        # Between condition with left exclusive and right inclusive.
    }
}