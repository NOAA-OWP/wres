package wres.pipeline.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import wres.config.MetricConstants;
import wres.config.DeclarationInterpolator;
import wres.config.components.BaselineDataset;
import wres.config.components.BaselineDatasetBuilder;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.Metric;
import wres.config.components.MetricBuilder;
import wres.config.components.MetricParameters;
import wres.config.components.MetricParametersBuilder;
import wres.config.components.Threshold;
import wres.config.components.ThresholdBuilder;
import wres.config.components.TimeInterval;
import wres.config.components.TimeIntervalBuilder;
import wres.config.components.TimePools;
import wres.config.components.TimePoolsBuilder;
import wres.config.components.VariableBuilder;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;

/**
 * Generates declarations for integration testing.
 * @author James Brown
 */

class TestDeclarationGenerator
{
    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static EvaluationDeclaration getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools()
    {
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( "2551-03-17T00:00:00Z" ) )
                                                         .maximum( Instant.parse( "2551-03-20T00:00:00Z" ) )
                                                         .build();

        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();

        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( "DISCHARGE" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "STREAMFLOW" )
                                                                .build() )
                                      .build();

        Set<Threshold> thresholds = new HashSet<>();
        wres.statistics.generated.Threshold threshold =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( 0.5 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold thresholdOuter = ThresholdBuilder.builder()
                                                   .threshold( threshold )
                                                   .type( wres.config.components.ThresholdType.VALUE )
                                                   .build();
        thresholds.add( thresholdOuter );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .features( features )
                                            .referenceDates( referenceDates )
                                            .referenceDatePools( Collections.singleton( referenceTimePools ) )
                                            .unit( "CFS" )
                                            .thresholds( thresholds )
                                            .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                            .minimumSampleSize( 0 )
                                            .metrics( Set.of() ) // All valid
                                            .build();
        return DeclarationInterpolator.interpolate( declaration, false );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics and issued date pools.
     */

    static EvaluationDeclaration getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools()
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "STREAMFLOW" )
                                                                .build() )
                                      .build();

        Dataset baselineDataset = DatasetBuilder.builder()
                                                .build();

        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .build();

        EvaluationDeclaration ensembleDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                                .right( right )
                                                                                .baseline( baseline )
                                                                                .metrics( Set.of() ) // All valid
                                                                                .build();
        return DeclarationInterpolator.interpolate( ensembleDeclaration, false );
    }

    /**
     * @return a declaration for single-valued forecasts without thresholds and with nominated metrics.
     */

    static EvaluationDeclaration getDeclarationForSingleValuedForecastsWithoutThresholds()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( "DISCHARGE" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "STREAMFLOW" )
                                                                .build() )
                                      .build();

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ),
                                      new Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, null ),
                                      new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ),
                                      new Metric( MetricConstants.BIAS_FRACTION, null ),
                                      new Metric( MetricConstants.COEFFICIENT_OF_DETERMINATION, null ),
                                      new Metric( MetricConstants.ROOT_MEAN_SQUARE_ERROR, null ),
                                      new Metric( MetricConstants.VOLUMETRIC_EFFICIENCY, null ),
                                      new Metric( MetricConstants.BOX_PLOT_OF_ERRORS, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .unit( "CFS" )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, false );
    }

    /**
     * @return a declaration for single-valued forecasts with thresholds and with nominated metrics.
     */

    static EvaluationDeclaration getDeclarationForSingleValuedForecastsWithThresholds()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( "QINE" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "SQIN" )
                                                                .build() )
                                      .build();

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder()
                                           .geometries( Set.of( geometryTuple ) )
                                           .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ),
                                      new Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, null ),
                                      new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ),
                                      new Metric( MetricConstants.MEAN_SQUARE_ERROR, null ),
                                      new Metric( MetricConstants.BIAS_FRACTION, null ),
                                      new Metric( MetricConstants.COEFFICIENT_OF_DETERMINATION, null ),
                                      new Metric( MetricConstants.ROOT_MEAN_SQUARE_ERROR, null ),
                                      new Metric( MetricConstants.THREAT_SCORE, null ),
                                      new Metric( MetricConstants.CONTINGENCY_TABLE, null ),
                                      new Metric( MetricConstants.QUANTILE_QUANTILE_DIAGRAM, null ),
                                      new Metric( MetricConstants.SAMPLE_SIZE, null ) );

        wres.statistics.generated.Threshold threshold =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( 4.9 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold thresholdOuter = ThresholdBuilder.builder()
                                                   .threshold( threshold )
                                                   .type( wres.config.components.ThresholdType.VALUE )
                                                   .build();
        Set<Threshold> valueThresholds = Set.of( thresholdOuter );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .unit( "CMS" )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( metrics )
                                                                        .thresholds( valueThresholds )
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, false );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static EvaluationDeclaration getDeclarationForSingleValuedForecastsWithAllValidMetrics()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( "DISCHARGE" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "STREAMFLOW" )
                                                                .build() )
                                      .build();

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        wres.statistics.generated.Threshold threshold =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( 0.5 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold thresholdOuter = ThresholdBuilder.builder()
                                                   .threshold( threshold )
                                                   .type( wres.config.components.ThresholdType.VALUE )
                                                   .build();
        Set<Threshold> valueThresholds = Set.of( thresholdOuter );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .unit( "CMS" )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( Set.of() )
                                                                        .thresholds( valueThresholds )
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, false );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static EvaluationDeclaration getDeclarationForSingleValuedForecastsWithTimeSeriesSummaryStatistics()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( "DISCHARGE" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "STREAMFLOW" )
                                                                .build() )
                                      .build();

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        SummaryStatistic.Builder template = SummaryStatistic.newBuilder()
                                                            .addDimension( SummaryStatistic.StatisticDimension.FEATURES );

        MetricParameters metricParameters = MetricParametersBuilder.builder()
                                                                   .summaryStatistics( Set.of( template.setStatistic(
                                                                                                               SummaryStatistic.StatisticName.MEAN )
                                                                                                       .build(),
                                                                                               template.setStatistic(
                                                                                                               SummaryStatistic.StatisticName.MEDIAN )
                                                                                                       .build(),
                                                                                               template.setStatistic(
                                                                                                               SummaryStatistic.StatisticName.MINIMUM )
                                                                                                       .build(),
                                                                                               template.setStatistic(
                                                                                                               SummaryStatistic.StatisticName.MAXIMUM )
                                                                                                       .build(),
                                                                                               template.setStatistic(
                                                                                                               SummaryStatistic.StatisticName.MEAN_ABSOLUTE )
                                                                                                       .build() ) )
                                                                   .build();

        Metric metric = MetricBuilder.builder()
                                     .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                     .parameters( metricParameters )
                                     .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .unit( "CMS" )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( Set.of( metric ) )
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, false );
    }

    /**
     * @return a declaration for ensemble forecasts with all valid metrics and value thresholds.
     */

    static EvaluationDeclaration getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds()
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        Set<Threshold> thresholds = new HashSet<>();
        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( 0 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.statistics.generated.Threshold two = one.toBuilder()
                                                     .setLeftThresholdValue( 5 )
                                                     .build();
        wres.statistics.generated.Threshold three = one.toBuilder()
                                                       .setLeftThresholdValue( 10 )
                                                       .build();
        wres.statistics.generated.Threshold four = one.toBuilder()
                                                      .setLeftThresholdValue( 15 )
                                                      .build();
        wres.statistics.generated.Threshold five = one.toBuilder()
                                                      .setLeftThresholdValue( 20 )
                                                      .build();

        Threshold oneOuter = ThresholdBuilder.builder()
                                             .threshold( one )
                                             .type( wres.config.components.ThresholdType.VALUE )
                                             .build();
        Threshold twoOuter = ThresholdBuilder.builder()
                                             .threshold( two )
                                             .type( wres.config.components.ThresholdType.VALUE )
                                             .build();
        Threshold threeOuter = ThresholdBuilder.builder()
                                               .threshold( three )
                                               .type( wres.config.components.ThresholdType.VALUE )
                                               .build();
        Threshold fourOuter = ThresholdBuilder.builder()
                                              .threshold( four )
                                              .type( wres.config.components.ThresholdType.VALUE )
                                              .build();
        Threshold fiveOuter = ThresholdBuilder.builder()
                                              .threshold( five )
                                              .type( wres.config.components.ThresholdType.VALUE )
                                              .build();

        thresholds.add( oneOuter );
        thresholds.add( twoOuter );
        thresholds.add( threeOuter );
        thresholds.add( fourOuter );
        thresholds.add( fiveOuter );

        EvaluationDeclaration ensembleDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                                .metrics( Set.of() ) // All valid
                                                                                .thresholds( thresholds )
                                                                                .build();

        return DeclarationInterpolator.interpolate( ensembleDeclaration, false );
    }

    /**
     * @return a declaration for ensemble forecasts without thresholds.
     */

    static EvaluationDeclaration getDeclarationForEnsembleForecastsWithoutThresholds()
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ),
                                      new Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, null ),
                                      new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ),
                                      new Metric( MetricConstants.BIAS_FRACTION, null ),
                                      new Metric( MetricConstants.COEFFICIENT_OF_DETERMINATION, null ),
                                      new Metric( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE, null ),
                                      new Metric( MetricConstants.ROOT_MEAN_SQUARE_ERROR, null ),
                                      new Metric( MetricConstants.TIME_TO_PEAK_ERROR, null ) );

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                                  .thresholds( Set.of() )
                                                                                  .metrics( metrics )
                                                                                  .build();

        return DeclarationInterpolator.interpolate( evaluationDeclaration, false );
    }

    /**
     * @return a declaration for ensemble forecasts with probability thresholds.
     */

    static EvaluationDeclaration getDeclarationForEnsembleForecastsWithProbabilityThresholds()
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ),
                                      new Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, null ),
                                      new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ),
                                      new Metric( MetricConstants.BIAS_FRACTION, null ),
                                      new Metric( MetricConstants.COEFFICIENT_OF_DETERMINATION, null ),
                                      new Metric( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE, null ),
                                      new Metric( MetricConstants.ROOT_MEAN_SQUARE_ERROR, null ),
                                      new Metric( MetricConstants.BRIER_SKILL_SCORE, null ),
                                      new Metric( MetricConstants.RANK_HISTOGRAM, null ),
                                      new Metric( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM, null ),
                                      new Metric( MetricConstants.SAMPLE_SIZE, null ) );

        Set<Threshold> thresholds = new HashSet<>();
        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( 0.0 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER_EQUAL )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.statistics.generated.Threshold two = one.toBuilder()
                                                     .setLeftThresholdProbability( 0.3919 )
                                                     .build();
        wres.statistics.generated.Threshold three = one.toBuilder()
                                                       .setLeftThresholdProbability( 0.4415 )
                                                       .build();
        wres.statistics.generated.Threshold four = one.toBuilder()
                                                      .setLeftThresholdProbability( 0.5042 )
                                                      .build();
        wres.statistics.generated.Threshold five = one.toBuilder()
                                                      .setLeftThresholdProbability( 0.5525 )
                                                      .build();

        Threshold oneOuter = ThresholdBuilder.builder()
                                             .threshold( one )
                                             .type( wres.config.components.ThresholdType.PROBABILITY )
                                             .build();
        Threshold twoOuter = ThresholdBuilder.builder()
                                             .threshold( two )
                                             .type( wres.config.components.ThresholdType.PROBABILITY )
                                             .build();
        Threshold threeOuter = ThresholdBuilder.builder()
                                               .threshold( three )
                                               .type( wres.config.components.ThresholdType.PROBABILITY )
                                               .build();
        Threshold fourOuter = ThresholdBuilder.builder()
                                              .threshold( four )
                                              .type( wres.config.components.ThresholdType.PROBABILITY )
                                              .build();
        Threshold fiveOuter = ThresholdBuilder.builder()
                                              .threshold( five )
                                              .type( wres.config.components.ThresholdType.PROBABILITY )
                                              .build();

        thresholds.add( oneOuter );
        thresholds.add( twoOuter );
        thresholds.add( threeOuter );
        thresholds.add( fourOuter );
        thresholds.add( fiveOuter );

        EvaluationDeclaration ensembleDecaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                               .thresholds( Set.of() )
                                                                               .probabilityThresholds( thresholds )
                                                                               .metrics( metrics )
                                                                               .build();
        return DeclarationInterpolator.interpolate( ensembleDecaration, false );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static EvaluationDeclaration getDeclarationForEnsembleForecastsWithAllValidMetrics()
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        return EvaluationDeclarationBuilder.builder( declaration )
                                           .referenceDates( null )
                                           .referenceDatePools( null )
                                           .build();
    }

    /**
     * @return a declaration for ensemble forecasts with a contingency table and value thresholds.
     */

    static EvaluationDeclaration getDeclarationForEnsembleForecastsWithContingencyTableAndValueThresholds()
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.CONTINGENCY_TABLE, null ) );

        Set<Threshold> probabilityThresholds = new HashSet<>();
        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( 0.05 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.statistics.generated.Threshold two = one.toBuilder()
                                                     .setLeftThresholdProbability( 0.25 )
                                                     .build();
        wres.statistics.generated.Threshold three = one.toBuilder()
                                                       .setLeftThresholdProbability( 0.5 )
                                                       .build();
        wres.statistics.generated.Threshold four = one.toBuilder()
                                                      .setLeftThresholdProbability( 0.75 )
                                                      .build();
        wres.statistics.generated.Threshold five = one.toBuilder()
                                                      .setLeftThresholdProbability( 0.9 )
                                                      .build();
        wres.statistics.generated.Threshold six = one.toBuilder()
                                                     .setLeftThresholdProbability( 0.95 )
                                                     .build();

        Threshold oneOuter = ThresholdBuilder.builder()
                                             .threshold( one )
                                             .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                             .build();
        Threshold twoOuter = ThresholdBuilder.builder()
                                             .threshold( two )
                                             .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                             .build();
        Threshold threeOuter = ThresholdBuilder.builder()
                                               .threshold( three )
                                               .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                               .build();
        Threshold fourOuter = ThresholdBuilder.builder()
                                              .threshold( four )
                                              .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                              .build();
        Threshold fiveOuter = ThresholdBuilder.builder()
                                              .threshold( five )
                                              .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                              .build();
        Threshold sixOuter = ThresholdBuilder.builder()
                                             .threshold( six )
                                             .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                             .build();

        probabilityThresholds.add( oneOuter );
        probabilityThresholds.add( twoOuter );
        probabilityThresholds.add( threeOuter );
        probabilityThresholds.add( fourOuter );
        probabilityThresholds.add( fiveOuter );
        probabilityThresholds.add( sixOuter );

        Set<Threshold> valueThresholds = new HashSet<>();
        wres.statistics.generated.Threshold seven =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( 50.0 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold sevenOuter = ThresholdBuilder.builder()
                                               .threshold( seven )
                                               .type( wres.config.components.ThresholdType.VALUE )
                                               .build();
        valueThresholds.add( sevenOuter );

        EvaluationDeclaration ensembleDeclaration =
                EvaluationDeclarationBuilder.builder( declaration )
                                            .referenceDatePools( null )
                                            .metrics( metrics )
                                            .thresholds( valueThresholds )
                                            .classifierThresholds( probabilityThresholds )
                                            .build();

        return DeclarationInterpolator.interpolate( ensembleDeclaration, false );
    }

    /**
     * Do not construct.
     */

    private TestDeclarationGenerator()
    {
    }
}
