@0xf4e64c48a6995a66;

using Java = import "capnpJava/java.capnp";
using Pool = import "pool.capnp".Pool;
using ScoreStatistic = import "scorestatistic.capnp".ScoreStatistic;
using DiagramStatistic = import "diagramstatistic.capnp".DiagramStatistic;
using BoxplotStatistic = import "boxplotstatistic.capnp".BoxplotStatistic;
using DurationScoreStatistic = import "durationscorestatistic.capnp".DurationScoreStatistic;
using DurationDiagramStatistic = import "durationdiagramstatistic.capnp".DurationDiagramStatistic;
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("StatisticsOuter");

struct Statistics
{
    # A message that encapsulates an atomic collection of statistics, namely the 
    # statistics associated with one pool. An evaluation identifier is required to 
    # connect a Statistics message to an Evaluation message. It is assumed that this 
    # identifier is packaged with the protocol; it is not provided inband.

    pool @0 :Pool;
    # The pool whose pairs were used to create the statistics here.
    
    scores @1 :List(ScoreStatistic);
    # Zero or more score statistics.

    diagrams @2 :List(DiagramStatistic);
    # Zero or more diagram statistics.

    boxPlots @3 :List(BoxplotStatistic);
    # Zero or more boxplot statistics.

    durationScores @4 :List(DurationScoreStatistic);
    # Zero or more duration score statistics.

    durationDiagrams @5 :List(DurationDiagramStatistic);
    # Zero or more duration diagram statistics.
}