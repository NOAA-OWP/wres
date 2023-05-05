package wres.pipeline.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import com.google.protobuf.DoubleValue;

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;

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
                                                   .setLeftThresholdValue( DoubleValue.of( 0.5 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold thresholdOuter = ThresholdBuilder.builder()
                                                   .threshold( threshold )
                                                   .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                   .build();
        thresholds.add( thresholdOuter );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .unit( "CFS" )
                                                                        .valueThresholds( thresholds )
                                                                        .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( Set.of() ) // All valid
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, true );
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
        return DeclarationInterpolator.interpolate( ensembleDeclaration, true );
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
        return DeclarationInterpolator.interpolate( declaration, true );
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
                                                   .setLeftThresholdValue( DoubleValue.of( 4.9 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold thresholdOuter = ThresholdBuilder.builder()
                                                   .threshold( threshold )
                                                   .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                   .build();
        Set<Threshold> valueThresholds = Set.of( thresholdOuter );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .unit( "CMS" )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( metrics )
                                                                        .valueThresholds( valueThresholds )
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, true );
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
                                                   .setLeftThresholdValue( DoubleValue.of( 0.5 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold thresholdOuter = ThresholdBuilder.builder()
                                                   .threshold( threshold )
                                                   .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                   .build();
        Set<Threshold> valueThresholds = Set.of( thresholdOuter );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .unit( "CMS" )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( Set.of() )
                                                                        .valueThresholds( valueThresholds )
                                                                        .build();
        return DeclarationInterpolator.interpolate( declaration, true );
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

        MetricParameters metricParameters = MetricParametersBuilder.builder()
                .summaryStatistics( Set.of( MetricConstants.MEAN,
                                            MetricConstants.MEDIAN,
                                            MetricConstants.MINIMUM,
                                            MetricConstants.MAXIMUM,
                                            MetricConstants.MEAN_ABSOLUTE ) )
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
        return DeclarationInterpolator.interpolate( declaration, true );
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
                                                   .setLeftThresholdValue( DoubleValue.of( 0 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.statistics.generated.Threshold two = one.toBuilder()
                                                     .setLeftThresholdValue( DoubleValue.of( 5 ) )
                                                     .build();
        wres.statistics.generated.Threshold three = one.toBuilder()
                                                       .setLeftThresholdValue( DoubleValue.of( 10 ) )
                                                       .build();
        wres.statistics.generated.Threshold four = one.toBuilder()
                                                      .setLeftThresholdValue( DoubleValue.of( 15 ) )
                                                      .build();
        wres.statistics.generated.Threshold five = one.toBuilder()
                                                      .setLeftThresholdValue( DoubleValue.of( 20 ) )
                                                      .build();

        Threshold oneOuter = ThresholdBuilder.builder()
                                             .threshold( one )
                                             .type( wres.config.yaml.components.ThresholdType.VALUE )
                                             .build();
        Threshold twoOuter = ThresholdBuilder.builder()
                                             .threshold( two )
                                             .type( wres.config.yaml.components.ThresholdType.VALUE )
                                             .build();
        Threshold threeOuter = ThresholdBuilder.builder()
                                               .threshold( three )
                                               .type( wres.config.yaml.components.ThresholdType.VALUE )
                                               .build();
        Threshold fourOuter = ThresholdBuilder.builder()
                                              .threshold( four )
                                              .type( wres.config.yaml.components.ThresholdType.VALUE )
                                              .build();
        Threshold fiveOuter = ThresholdBuilder.builder()
                                              .threshold( five )
                                              .type( wres.config.yaml.components.ThresholdType.VALUE )
                                              .build();

        thresholds.add( oneOuter );
        thresholds.add( twoOuter );
        thresholds.add( threeOuter );
        thresholds.add( fourOuter );
        thresholds.add( fiveOuter );

        EvaluationDeclaration ensembleDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                                .metrics( Set.of() ) // All valid
                                                                                .valueThresholds( thresholds )
                                                                                .build();

        return DeclarationInterpolator.interpolate( ensembleDeclaration, true );
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
                                      new Metric( MetricConstants.ROOT_MEAN_SQUARE_ERROR, null ) );

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                                  .valueThresholds( Set.of() )
                                                                                  .metrics( metrics )
                                                                                  .build();

        return DeclarationInterpolator.interpolate( evaluationDeclaration, true );
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
                                                   .setLeftThresholdProbability( DoubleValue.of( 0.0 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER_EQUAL )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.statistics.generated.Threshold two = one.toBuilder()
                                                     .setLeftThresholdProbability( DoubleValue.of( 0.3919 ) )
                                                     .build();
        wres.statistics.generated.Threshold three = one.toBuilder()
                                                       .setLeftThresholdProbability( DoubleValue.of( 0.4415 ) )
                                                       .build();
        wres.statistics.generated.Threshold four = one.toBuilder()
                                                      .setLeftThresholdProbability( DoubleValue.of( 0.5042 ) )
                                                      .build();
        wres.statistics.generated.Threshold five = one.toBuilder()
                                                      .setLeftThresholdProbability( DoubleValue.of( 0.5525 ) )
                                                      .build();

        Threshold oneOuter = ThresholdBuilder.builder()
                                             .threshold( one )
                                             .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                             .build();
        Threshold twoOuter = ThresholdBuilder.builder()
                                             .threshold( two )
                                             .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                             .build();
        Threshold threeOuter = ThresholdBuilder.builder()
                                               .threshold( three )
                                               .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                               .build();
        Threshold fourOuter = ThresholdBuilder.builder()
                                              .threshold( four )
                                              .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                              .build();
        Threshold fiveOuter = ThresholdBuilder.builder()
                                              .threshold( five )
                                              .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                              .build();

        thresholds.add( oneOuter );
        thresholds.add( twoOuter );
        thresholds.add( threeOuter );
        thresholds.add( fourOuter );
        thresholds.add( fiveOuter );

        EvaluationDeclaration ensembleDecaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                               .valueThresholds( Set.of() )
                                                                               .probabilityThresholds( thresholds )
                                                                               .metrics( metrics )
                                                                               .build();
        return DeclarationInterpolator.interpolate( ensembleDecaration, true );
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
                                                   .setLeftThresholdProbability( DoubleValue.of( 0.05 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.statistics.generated.Threshold two = one.toBuilder()
                                                     .setLeftThresholdProbability( DoubleValue.of( 0.25 ) )
                                                     .build();
        wres.statistics.generated.Threshold three = one.toBuilder()
                                                       .setLeftThresholdProbability( DoubleValue.of( 0.5 ) )
                                                       .build();
        wres.statistics.generated.Threshold four = one.toBuilder()
                                                      .setLeftThresholdProbability( DoubleValue.of( 0.75 ) )
                                                      .build();
        wres.statistics.generated.Threshold five = one.toBuilder()
                                                      .setLeftThresholdProbability( DoubleValue.of( 0.9 ) )
                                                      .build();
        wres.statistics.generated.Threshold six = one.toBuilder()
                                                     .setLeftThresholdProbability( DoubleValue.of( 0.95 ) )
                                                     .build();

        Threshold oneOuter = ThresholdBuilder.builder()
                                             .threshold( one )
                                             .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                             .build();
        Threshold twoOuter = ThresholdBuilder.builder()
                                             .threshold( two )
                                             .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                             .build();
        Threshold threeOuter = ThresholdBuilder.builder()
                                               .threshold( three )
                                               .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                               .build();
        Threshold fourOuter = ThresholdBuilder.builder()
                                              .threshold( four )
                                              .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                              .build();
        Threshold fiveOuter = ThresholdBuilder.builder()
                                              .threshold( five )
                                              .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                              .build();
        Threshold sixOuter = ThresholdBuilder.builder()
                                             .threshold( six )
                                             .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
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
                                                   .setLeftThresholdValue( DoubleValue.of( 50.0 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        Threshold sevenOuter = ThresholdBuilder.builder()
                                               .threshold( seven )
                                               .type( wres.config.yaml.components.ThresholdType.VALUE )
                                               .build();
        valueThresholds.add( sevenOuter );

        EvaluationDeclaration ensembleDeclaration =
                EvaluationDeclarationBuilder.builder( declaration )
                                            .referenceDatePools( null )
                                            .metrics( metrics )
                                            .valueThresholds( valueThresholds )
                                            .classifierThresholds( probabilityThresholds )
                                            .build();

        return DeclarationInterpolator.interpolate( ensembleDeclaration, true );
    }

    /**
     * Do not construct.
     */

    private TestDeclarationGenerator()
    {
    }
}
