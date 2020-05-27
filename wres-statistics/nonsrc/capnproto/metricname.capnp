@0xc007e9776f4cbe8c;

using Java = import "java.capnp";
$Java.package("wres.statistics.generated.capnp");
$Java.outerClassname("MetricNameOuter");

enum MetricName
{
    # An enumeration of metric names. Would be preferable to alias the
    #  wres.config.generated.MetricConfigName, rather than have two generated 
    #  enumerations. Also coordinated with the wres.datamodel.MetricConstants, but
    #  that is a more complex enumeration with several additional behaviors.

    undefined @0;
    biasFraction @1;
    boxPlotOfErrors @2;
    boxPlotOfPercentageErrors @3;
    boxPlotOfErrorsByObservedValue @4;
    boxPlotOfErrorsByForecastValue @5;
    brierScore @6;
    brierSkillScore @7;
    coefficientOfDetermination @8;
    contingencyTable @9;
    pearsonCorrelationCoefficient @10;
    continuousRankedProbabilityScore @11;
    continuousRankedProbabilitySkillScore @12;
    threatScore @13;
    equitableThreatScore @14;
    frequencyBias @15;
    indexOfAgreement @16;
    klingGuptaEfficiency @17;
    meanAbsoluteError @18;
    meanError @19;
    meanSquareError @20;
    meanSquareErrorSkillScore @21;
    meanSquareErrorSkillScoreNormalized @22;
    medianError @23;
    peirceSkillScore @24;
    probabilityOfDetection @25;
    probabilityOfFalseDetection @26;
    quantileQuantileDiagram @27;
    rankHistogram @28;
    relativeOperatingCharacteristicDiagram @29;
    relativeOperatingCharacteristicScore @30;
    reliabilityDiagram @31;
    rootMeanSquareError @32;
    rootMeanSquareErrorNormalized @33;
    sampleSize @34;
    sumOfSquareError @35;
    volumetricEfficiency @36;
    timeToPeakError @37;
    timeToPeakRelativeError @38;
    timeToPeakErrorStatistic @39;
    timeToPeakRelativeErrorStatistic @40;
}