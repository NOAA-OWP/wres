@0xee1fd294213e42d6;

using Java = import "capnpJava/java.capnp";
using TimeScale = import "timescale.capnp".TimeScale;
using ValueFilter = import "valuefilter.capnp".ValueFilter;
using Season = import "season.capnp".Season;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("EvaluationOuter");

struct Evaluation
{
    # A message that encapsulates an evaluation. An evaluation is composed of N
    # pools of pairs and M metrics. The pairs represent a single variable and time-
    # scale. Each pool of pairs is composed of G geospatial features and one time 
    # window. In the absence of thresholds that further partition a pool, each 
    # evaluation produces N*M statistics.
    
    poolMessageCount @0 :Int32;
    # Number of pools per evaluation.
    
    metricMessageCount @1 :Int32;
    # Number of metrics per evaluation. Always positive.

    leftSourceName @2 :Text; 
    rightSourceName @3 :Text; 
    baselineSourceName @4 :Text; 
    variableName @5 :Text;
    measurementUnit @6 :Text;
    season @7 :Season;
    timeScale @8 :TimeScale;

    ensembleMemberSubset @9 :List(Text);
    # A subset of ensemble members by name that were filtered.

    valueFilter @10 :ValueFilter;
}