package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.CovariateDatasetBuilder;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.CrossPairMethod;
import wres.config.yaml.components.CrossPairScope;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.SamplingUncertainty;
import wres.config.yaml.components.SamplingUncertaintyBuilder;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimeScaleBuilder;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.Variable;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link DeclarationValidator}.
 * @author James Brown
 */
class DeclarationValidatorTest
{
    /** A default dataset for re-use. */
    private Dataset defaultDataset = null;

    @BeforeEach
    void runBeforeEach() throws URISyntaxException
    {
        Source source = SourceBuilder.builder()
                                     .uri( new URI( "http://foo" ) )
                                     .build();

        this.defaultDataset = DatasetBuilder.builder()
                                            .sources( List.of( source ) )
                                            .build();
    }

    @Test
    void testLeftAndRightAreNotBothEnsemblesResultsInError()
    {
        Dataset ensembles = DatasetBuilder.builder()
                                          .type( DataType.ENSEMBLE_FORECASTS )
                                          .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( ensembles )
                                                                        .right( ensembles )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "Cannot evaluate ensemble forecasts",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testVariablesAreDeclaredWhenRequiredResultsInError()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.USGS_NWIS )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .sourceInterface( SourceInterface.WRDS_NWM )
                                            .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( source, anotherSource ) )
                                        .type( DataType.OBSERVATIONS )
                                        .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .build();
        List<CovariateDataset> covariates = List.of( new CovariateDataset( dataset, null, null ) );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
                                                                        .baseline( baseline )
                                                                        .covariates( covariates )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'observed' dataset "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'observed' dataset "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'predicted' dataset "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'predicted' dataset "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'baseline' dataset "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'baseline' dataset "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for a 'covariate' dataset "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for a 'covariate' dataset "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) )

        );
    }

    @Test
    void testNonUniqueVariableNamesForCovariatesResultsInError()
    {
        Variable one = new Variable( "foo", null );
        Variable two = new Variable( "foo", null );

        Dataset dataOne = DatasetBuilder.builder( this.defaultDataset )
                                        .variable( one )
                                        .build();
        CovariateDataset covariateOne = new CovariateDataset( dataOne, null, null );
        Dataset dataTwo = DatasetBuilder.builder( this.defaultDataset )
                                        .variable( two )
                                        .build();
        CovariateDataset covariateTwo = new CovariateDataset( dataTwo, null, null );

        List<CovariateDataset> covariates = List.of( covariateOne, covariateTwo );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .covariates( covariates )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "The duplicate names were: [foo].",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testForecastDataTypeForCovariatesResultsInError()
    {
        Dataset dataOne = DatasetBuilder.builder( this.defaultDataset )
                                        .type( DataType.OBSERVATIONS )
                                        .build();
        CovariateDataset covariateOne = new CovariateDataset( dataOne, null, null );
        Dataset dataTwo = DatasetBuilder.builder( this.defaultDataset )
                                        .type( DataType.SINGLE_VALUED_FORECASTS )
                                        .build();
        CovariateDataset covariateTwo = new CovariateDataset( dataTwo, null, null );

        List<CovariateDataset> covariates = List.of( covariateOne, covariateTwo );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .covariates( covariates )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "Discovered a forecast data 'type' for one or "
                                                               + "more 'covariate' datasets",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testDatesAreDeclaredForWebServiceSourcesResultsInErrors()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.USGS_NWIS )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .sourceInterface( SourceInterface.WRDS_NWM )
                                            .build();
        Source yetAnotherSource = SourceBuilder.builder()
                                               .sourceInterface( SourceInterface.WRDS_AHPS )
                                               .build();
        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( anotherSource ) )
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();
        Dataset baselineInner = DatasetBuilder.builder()
                                              .sources( List.of( yetAnotherSource ) )
                                              .type( DataType.SINGLE_VALUED_FORECASTS )
                                              .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineInner )
                                                         .build();

        CovariateDataset covariate = new CovariateDataset( left, null, null );
        List<CovariateDataset> covariates = List.of( covariate );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .baseline( baseline )
                                                                        .covariates( covariates )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "'observed' data sources that "
                                                                                + "have a data type of 'observations' "
                                                                                + "and use web services, but the "
                                                                                + "'valid_dates' were incomplete",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "'predicted' data sources that "
                                                                                + "have a data type of 'ensemble "
                                                                                + "forecasts' and use web services, "
                                                                                + "but the 'reference_dates' were "
                                                                                + "incomplete",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "'baseline' data sources that "
                                                                                + "have a data type of 'single valued "
                                                                                + "forecasts' and use web services, "
                                                                                + "but the 'reference_dates' were "
                                                                                + "incomplete",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "'covariate' data sources that "
                                                                                + "have a data type of 'observations' "
                                                                                + "and use web services, but the "
                                                                                + "'valid_dates' were incomplete",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testValidateDeclarationWithoutDataSourcesResultsInNoErrorsWhenOmittingSourceValidation()
    {
        Dataset dataset = DatasetBuilder.builder()
                                        .type( DataType.OBSERVATIONS )
                                        .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
                                                                        .baseline( baseline )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration, true );

        assertTrue( events.stream()
                          .noneMatch( e -> e.getStatusLevel() == StatusLevel.ERROR ) );
    }

    @Test
    void testConflictingTimeZoneForSourceResultsInWarningsAndError()
    {
        Source source = SourceBuilder.builder()
                                     // Warning
                                     .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            // Warning
                                            .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                            .build();
        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .type( DataType.OBSERVATIONS )
                                     // Error
                                     .timeZoneOffset( ZoneOffset.ofHours( -4 ) )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( anotherSource ) )
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Dataset covariate = DatasetBuilder.builder()
                                          .sources( List.of( source ) )
                                          .type( DataType.OBSERVATIONS )
                                          // Error
                                          .timeZoneOffset( ZoneOffset.ofHours( -7 ) )
                                          .build();

        CovariateDataset covariateDataset = new CovariateDataset( covariate, null, null );

        List<CovariateDataset> covariates = List.of( covariateDataset );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .covariates( covariates )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "Discovered one or more "
                                                                                + "'observed' data sources for which a "
                                                                                + "time zone was declared",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "Discovered one or more "
                                                                                + "'predicted' data sources for which "
                                                                                + "a time zone was declared",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'observed' dataset, "
                                                                                + "which is inconsistent with some of "
                                                                                + "the",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for a 'covariate' dataset, "
                                                                                + "which is inconsistent with some of "
                                                                                + "the",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testInterfacesAreConsistentWithDataTypesResultsInErrorsAndWarnings()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.USGS_NWIS )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .sourceInterface( SourceInterface.WRDS_AHPS )
                                            .build();
        Source yetAnotherSource = SourceBuilder.builder()
                                               .sourceInterface( SourceInterface.WRDS_AHPS )
                                               .build();
        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .type( DataType.SINGLE_VALUED_FORECASTS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( anotherSource ) )
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();
        Dataset baselineInner = DatasetBuilder.builder()
                                              .sources( List.of( yetAnotherSource ) )
                                              .type( DataType.SIMULATIONS )
                                              .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineInner )
                                                         .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .baseline( baseline )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "but the declared or inferred "
                                                                                + "data type for the observed data was "
                                                                                + "single valued forecasts, which is "
                                                                                + "inconsistent with the interface",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, " but the declared or inferred "
                                                                                + "data type for the predicted data "
                                                                                + "was ensemble forecasts, which is "
                                                                                + "inconsistent with the interface",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "but the declared or inferred "
                                                                                + "data type for the baseline data was "
                                                                                + "simulations, which is inconsistent "
                                                                                + "with the interface",
                                                                        StatusLevel.WARN ) )
        );
    }

    @Test
    void testTypesAreConsistentWithEnsembleDeclarationResultsinError()
    {
        Source source = SourceBuilder.builder()
                                     .build();

        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( source ) )
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .ensembleFilter( new EnsembleFilter( Set.of( "1002" ), false ) )
                                      .build();

        Set<Metric> metrics =
                Set.of( new Metric( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE, null ) );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                        .metrics( metrics )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "but some of the declaration is designed for this data "
                                                       + "type: [An 'ensemble_average' was declared., An "
                                                       + "'ensemble_filter' was declared on the predicted dataset., "
                                                       + "Discovered metrics that require ensemble forecasts: "
                                                       + "[CONTINUOUS RANKED PROBABILITY SCORE].]",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testTypesAreConsistentWithForecastDeclarationResultsinError()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.NWM_SHORT_RANGE_CHANNEL_RT_CONUS )
                                     .build();

        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( source ) )
                                      .type( DataType.SIMULATIONS )
                                      .build();

        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( left )
                                              .right( right )
                                              .referenceDates( new TimeInterval( Instant.MIN, Instant.MAX ) )
                                              .referenceDatePools( new TimePools( java.time.Duration.ofHours( 1 ),
                                                                                  null ) )
                                              .leadTimes( new LeadTimeInterval( java.time.Duration.ofHours( 0 ),
                                                                                java.time.Duration.ofHours( 1 ) ) )
                                              .leadTimePools( new TimePools( java.time.Duration.ofHours( 1 ),
                                                                             null ) )
                                              .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "but some of the declaration is designed for these "
                                                               + "data types: [Discovered 'lead_time_pool'., "
                                                               + "Discovered 'lead_times' filter., Discovered "
                                                               + "'reference_date_pools'., Discovered a "
                                                               + "'reference_dates' filter., Discovered one or more "
                                                               + "data sources whose interfaces are forecast-like: "
                                                               + "[nwm short range channel rt conus].]",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testEvaluationTimeScaleIsValidResultsinError()
    {
        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( this.defaultDataset )
                                              .right( this.defaultDataset )
                                              .timeScale( timeScale )
                                              .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "does not have a valid time "
                                                                                + "scale function",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "Either declare the 'period' "
                                                                                + "and 'unit' or declare a fully-"
                                                                                + "specified time scale season",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testEvaluateDatasetTimeScaleIsValidResultsinError()
    {
        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .setStartDay( 1 )
                                            .setEndDay( 1 )
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );

        Dataset left = DatasetBuilder.builder()
                                     .timeScale( timeScale )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .timeScale( timeScale )
                                      .build();
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( left )
                                              .right( right )
                                              .rescaleLenience( TimeScaleLenience.ALL )
                                              .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "does not have a valid time "
                                                                                + "scale function",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "Either declare the 'period' "
                                                                                + "and 'unit' or declare a fully-"
                                                                                + "specified time scale season",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "When including either a "
                                                                                + "'minimum_day' or a 'minimum_month', "
                                                                                + "both must be present",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "When including either a "
                                                                                + "'maximum_day' or a 'maximum_month', "
                                                                                + "both must be present",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The time scale 'period' must be "
                                                                        + "declared explicitly or a time scale season "
                                                                        + "fully defined",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "Care should be exercised when "
                                                                        + "performing lenient rescaling",
                                                                        StatusLevel.WARN ) )
        );
    }

    @Test
    void testEvaluationTimeScaleIsConsistentWithDatasetTimeScalesResultsInErrors()
    {
        TimeScale timeScaleInnerSource = TimeScale.newBuilder()
                                                  .setPeriod( Duration.newBuilder()
                                                                      .setSeconds( 23 )
                                                                      .build() )
                                                  .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                                  .build();
        wres.config.yaml.components.TimeScale timeScaleSource =
                new wres.config.yaml.components.TimeScale( timeScaleInnerSource );

        Dataset left = DatasetBuilder.builder()
                                     .timeScale( timeScaleSource )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .timeScale( timeScaleSource )
                                      .build();

        TimeScale covariateTimeScale = TimeScale.newBuilder()
                                                .setPeriod( Duration.newBuilder()
                                                                    .setSeconds( 100 )
                                                                    .build() )
                                                .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                                .build();

        wres.config.yaml.components.TimeScale timeScaleCovariate =
                new wres.config.yaml.components.TimeScale( covariateTimeScale );

        Dataset covariate = DatasetBuilder.builder()
                                          .timeScale( timeScaleCovariate )
                                          .build();

        CovariateDataset covariateDataset = new CovariateDataset( covariate, null, null );
        List<CovariateDataset> covariates = List.of( covariateDataset );

        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .setPeriod( Duration.newBuilder()
                                                                .setSeconds( 10 )
                                                                .build() )
                                            .setFunction( TimeScale.TimeScaleFunction.TOTAL )
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );

        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( left )
                                              .right( right )
                                              .covariates( covariates )
                                              .timeScale( timeScale )
                                              .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "cannot be instantaneous",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "is smaller than the "
                                                                                + "evaluation 'time_scale'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "is not exactly divisible",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "requires a total, but the "
                                                                                + "time scale associated with a "
                                                                                + "'covariate'",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testDuplicateUnitAliasesResultsInErrors()
    {
        Set<UnitAlias> unitAliases = Set.of( new UnitAlias( "foo", "bar" ),
                                             new UnitAlias( "foo", "baz" ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .unitAliases( unitAliases )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "discovered the same alias associated with "
                                                               + "multiple units", StatusLevel.ERROR ) );
    }

    @Test
    void testInvalidTimeIntervalsResultsInErrors()
    {
        TimeInterval interval = new TimeInterval( Instant.MAX, Instant.MIN );
        LeadTimeInterval leadInterval = new LeadTimeInterval( java.time.Duration.ofHours( 3 ),
                                                              java.time.Duration.ofHours( 1 ) );
        AnalysisTimes analysisTimes = new AnalysisTimes( java.time.Duration.ofHours( 1 ),
                                                         java.time.Duration.ZERO );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .referenceDates( interval )
                                                                        .validDates( interval )
                                                                        .leadTimes( leadInterval )
                                                                        .analysisTimes( analysisTimes )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "The 'reference_dates' "
                                                                                + "interval is invalid",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The 'valid_dates' interval is "
                                                                                + "invalid",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The 'lead_times' interval "
                                                                                + "is invalid",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The 'analysis_times' "
                                                                                + "interval is invalid",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testInvalidSeasonResultsInError()
    {
        Season season = new Season( MonthDay.of( 1, 1 ), MonthDay.of( 1, 1 ) );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .season( season )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "A season must span a non-zero "
                                                               + "time interval",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testInvalidSeasonResultsInWarning()
    {
        Season season = new Season( MonthDay.of( 1, 2 ), MonthDay.of( 1, 1 ) );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .season( season )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "Although it is valid to wrap "
                                                               + "a season",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testMissingTimeIntervalsWithTimePoolsResultsInErrors()
    {
        TimePools timePool = new TimePools( java.time.Duration.ofHours( 1 ),
                                            java.time.Duration.ofHours( 1 ) );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .validDatePools( timePool )
                                                                        .leadTimePools( timePool )
                                                                        .referenceDatePools( timePool )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "Please remove the "
                                                                                + "'lead_time_pools' or fully declare "
                                                                                + "the 'lead_times'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "Please remove the "
                                                                                + "'reference_date_pools' or fully "
                                                                                + "declare the 'reference_dates'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "Please remove the "
                                                                                + "'valid_date_pools' or fully declare "
                                                                                + "the 'valid_dates'",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testInvalidTimePoolsResultsInErrors()
    {
        TimePools timePool = new TimePools( java.time.Duration.ofHours( 1 ),
                                            null );
        TimeInterval interval = new TimeInterval( Instant.parse( "2047-01-01T00:00:00Z" ),
                                                  Instant.parse( "2047-01-01T00:01:00Z" ) );
        LeadTimeInterval leadInterval = new LeadTimeInterval( java.time.Duration.ofMinutes( 1 ),
                                                              java.time.Duration.ofMinutes( 2 ) );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .validDates( interval )
                                                                        .referenceDates( interval )
                                                                        .leadTimes( leadInterval )
                                                                        .validDatePools( timePool )
                                                                        .leadTimePools( timePool )
                                                                        .referenceDatePools( timePool )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "The declaration requested "
                                                                                + "'lead_time_pools', but none could "
                                                                                + "be produced because the 'minimum'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The declaration requested "
                                                                                + "'reference_date_pools', but none "
                                                                                + "could be produced because the "
                                                                                + "'minimum'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The declaration requested "
                                                                                + "'valid_date_pools', but none could "
                                                                                + "be produced because the 'minimum'",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testMissingFeaturesAndWebSourcesResultsInErrors()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.USGS_NWIS )
                                     .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( source ) )
                                        .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
                                                                        .baseline( baseline )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "No geospatial features were "
                                                                                + "defined, but web sources were "
                                                                                + "declared for the 'observed' dataset",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "No geospatial features were "
                                                                                + "defined, but web sources were "
                                                                                + "declared for the 'predicted' dataset",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "No geospatial features were "
                                                                                + "defined, but web sources were "
                                                                                + "declared for the 'baseline' dataset",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testMissingFeaturesAndNwmInterfaceResultsInErrors()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS )
                                     .build();
        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( source ) )
                                        .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
                                                                        .baseline( baseline )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "source interfaces that "
                                                                                + "require features were declared for "
                                                                                + "the 'observed' dataset",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "source interfaces that "
                                                                                + "require features were declared for "
                                                                                + "the 'predicted' dataset",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "source interfaces that "
                                                                                + "require features were declared for "
                                                                                + "the 'baseline' dataset",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testFeaturesIncludeBaselineAndMissingBaselineDatasetResultsInErrors()
    {
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setBaseline( Geometry.newBuilder()
                                                                                   .setName( "foo" ) )
                                                             .build() );

        Features features = new Features( geometries );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .features( features )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "The declaration contains one or more geospatial "
                                                               + "features for a 'baseline' dataset but no 'baseline' "
                                                               + "dataset is defined",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testEvaluationIncludesSparseFeaturesFromDifferentFeatureAuthoritiesAndNoFeatureService()
    {
        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .featureAuthority( FeatureAuthority.NWS_LID )
                                      .build();

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "foo" ) )
                                                             .build(),
                                                GeometryTuple.newBuilder()
                                                             .setRight( Geometry.newBuilder()
                                                                                .setName( "bar" ) )
                                                             .build() );

        Features features = new Features( geometries );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "but different feature authorities were "
                                                               + "detected for each side of data and no feature "
                                                               + "service was declared",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testCovariatesWithInconsistentFeatureAuthoritiesProducesError()
    {
        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .featureAuthority( FeatureAuthority.NWS_LID )
                                      .build();

        Dataset covariate = DatasetBuilder.builder()
                                          .featureAuthority( FeatureAuthority.NWM_FEATURE_ID )
                                          .build();

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "foo" ) )
                                                             .build(),
                                                GeometryTuple.newBuilder()
                                                             .setRight( Geometry.newBuilder()
                                                                                .setName( "bar" ) )
                                                             .build() );

        Features features = new Features( geometries );

        CovariateDataset covariateDataset = new CovariateDataset( covariate, null, null );
        List<CovariateDataset> covariates = List.of( covariateDataset );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .covariates( covariates )
                                                                        .features( features )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "The unrecognized feature authorities "
                                                       + "associated with 'covariate' datasets were: "
                                                       + "[nwm feature id]",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testTimeSeriesMetricsWithoutSingleValuedForecastsResultsInError()
    {
        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of() )
                                        .type( DataType.ENSEMBLE_FORECASTS )
                                        .build();

        Metric metric = new Metric( MetricConstants.TIME_TO_PEAK_ERROR, null );
        Set<Metric> metrics = Set.of( metric );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( dataset )
                                                                        .metrics( metrics )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, " the following metrics require single-valued "
                                                               + "forecasts",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testScoreThatNeedsBaselineWithNoBaselineResultsInError()
    {
        Metric metric = new Metric( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE, null );
        Set<Metric> metrics = Set.of( metric );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .metrics( metrics )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "The declaration includes metrics that require "
                                                               + "an explicit 'baseline' dataset",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testLegacyCsvWithDiagramMetricAndDatePoolsResultsInWarnings()
    {
        Metric metric = new Metric( MetricConstants.RELIABILITY_DIAGRAM, null );
        Set<Metric> metrics = Set.of( metric );
        TimePools timePool = new TimePools( java.time.Duration.ofHours( 3 ),
                                            java.time.Duration.ofHours( 1 ) );
        Outputs formats = Outputs.newBuilder()
                                 .setCsv( Outputs.CsvFormat.getDefaultInstance() )
                                 .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .validDatePools( timePool )
                                                                        .referenceDatePools( timePool )
                                                                        .metrics( metrics )
                                                                        .formats( new Formats( formats ) )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "does not support these metrics in "
                                                                        + "combination with 'reference_date_pools",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "does not support these metrics in "
                                                                        + "combination with 'valid_date_pools",
                                                                        StatusLevel.WARN ) )
        );
    }

    @Test
    void testCategoricalMetricsWithoutEventThresholdsResultsInWarningAndError()
    {
        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of() )
                                        .type( DataType.ENSEMBLE_FORECASTS )
                                        .build();
        Metric metric = new Metric( MetricConstants.PROBABILITY_OF_DETECTION, null );
        Set<Metric> metrics = Set.of( metric );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( dataset )
                                                                        .metrics( metrics )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The declaration includes metrics that "
                                                                        + "require either 'thresholds' or "
                                                                        + "'probability_thresholds' but none were "
                                                                        + "found",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The declaration includes ensemble "
                                                                        + "forecasts and metrics for categorical "
                                                                        + "datasets, but does not include any "
                                                                        + "'classifier_thresholds'",
                                                                        StatusLevel.WARN ) )
        );
    }

    @Test
    void testOutputFormatsWithDeprecatedOptionsResultsInWarnings()
    {
        Outputs formats = Outputs.newBuilder()
                                 .setCsv( Outputs.CsvFormat.getDefaultInstance() )
                                 .setNetcdf( Outputs.NetcdfFormat.getDefaultInstance() )
                                 .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .formats( new Formats( formats ) )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The evaluation requested the 'csv' "
                                                                        + "format, which has been marked deprecated",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The evaluation requested the 'netcdf' "
                                                                        + "format, which has been marked deprecated",
                                                                        StatusLevel.WARN ) )
        );
    }

    @Test
    void testInvalidNetcdfDeclarationResultsInErrorsAndWarnings()
    {
        Outputs formats = Outputs.newBuilder()
                                 .setNetcdf2( Outputs.Netcdf2Format.getDefaultInstance() )
                                 .setNetcdf( Outputs.NetcdfFormat.getDefaultInstance() )
                                 .build();
        FeatureGroups featureGroups = new FeatureGroups( Set.of() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .formats( new Formats( formats ) )
                                                                        .featureGroups( featureGroups )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The 'output_formats' includes both "
                                                                        + "'netcdf' and 'netcdf2', which is not allowed",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The 'output_formats' includes "
                                                                        + "'netcdf', which does not support "
                                                                        + "'feature_groups'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The 'output_formats' includes "
                                                                        + "'netcdf2', which supports 'feature_groups', "
                                                                        + "but",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "these formats only support the writing "
                                                                        + "of verification scores",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testInvalidThresholdServiceDeclarationResultsInErrors()
    {
        ThresholdSource service
                = ThresholdSourceBuilder.builder()
                                        .uri( URI.create( "http://foo" ) )
                                        .featureNameFrom( DatasetOrientation.BASELINE )
                                        .build();

        // No baseline feature = first error
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "foo" ) )
                                                             .build() );
        Features features = new Features( geometries );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        // No baseline = second error
                                                                        .features( features )
                                                                        .thresholdSources( Set.of( service ) )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "correlate features with thresholds, "
                                                                        + "but no 'baseline' dataset was discovered",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "feature(s) were discovered with a "
                                                                        + "missing 'baseline' feature name",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testInconsistentEnsembleAverageTypesResultsInAWarning()
    {
        Metric first =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                             .parameters( MetricParametersBuilder.builder()
                                                                 // Results in a warning
                                                                 .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                 .build() )
                             .build();

        Set<Metric> metrics = Set.of( first );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            // Results in a warning
                                            .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                            .metrics( metrics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "but all of the metrics that declare their own type "
                                                       + "will retain that type. The following metrics will not be "
                                                       + "adjusted: [MEAN SQUARE ERROR SKILL SCORE]",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void noDataSourcesDeclaredForAnyDatasetResultsInErrors()
    {
        Dataset left = DatasetBuilder.builder()
                                     .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( left )
                                                         .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( left )
                                            .baseline( baseline )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "No data sources were declared for the "
                                                                        + "'observed' dataset, which is not allowed",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "No data sources were declared for the "
                                                                        + "'predicted' dataset, which is not allowed",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "No data sources were declared for the "
                                                                        + "'baseline' dataset, which is not allowed",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testNoValidationErrorAboutClassifierThresholdsWhenMetricParametersAreSupplied()
    {
        Metric first =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                 .build() )
                             .build();

        Set<Metric> metrics = Set.of( first );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .metrics( metrics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertFalse( DeclarationValidatorTest.contains( events,
                                                        "declaration contains one or more "
                                                        + "'classifier_thresholds'",
                                                        StatusLevel.ERROR ) );
    }

    @Test
    void testInstantaneousTimeScaleForLeftSourceProducesNoErrors()
    {
        Dataset left = DatasetBuilder.builder( this.defaultDataset )
                                     .timeScale( TimeScaleBuilder.TimeScale( TimeScale.newBuilder()
                                                                                      .setPeriod( Duration.newBuilder()
                                                                                                          .setSeconds( 1 ) )
                                                                                      .build() ) )
                                     .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( this.defaultDataset )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertFalse( DeclarationValidatorTest.contains( events,
                                                        "cannot be instantaneous",
                                                        StatusLevel.ERROR ) );
    }

    @Test
    void testNonInstantaneousEvaluationTimeScaleProducesNoErrors()
    {
        wres.config.yaml.components.TimeScale timeScale =
                TimeScaleBuilder.TimeScale( TimeScale.newBuilder()
                                                     .setPeriod( Duration.newBuilder()
                                                                         .setSeconds( 24 * 60 * 60 ) )
                                                     .build() );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .timeScale( timeScale )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertFalse( DeclarationValidatorTest.contains( events,
                                                        "cannot be instantaneous",
                                                        StatusLevel.ERROR ) );
    }

    @Test
    void testThresholdServiceAllowsForValidationWithoutErrorWhenMetricsRequirethresholds()
    {
        Metric first =
                MetricBuilder.builder()
                             .name( MetricConstants.PROBABILITY_OF_DETECTION )
                             .build();

        Set<Metric> metrics = Set.of( first );
        ThresholdSource service = ThresholdSourceBuilder.builder()
                                                        .uri( URI.create( "http://foo.bar" ) )
                                                        .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .thresholdSources( Set.of( service ) )
                                            .metrics( metrics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertFalse( DeclarationValidatorTest.contains( events,
                                                        "'value_thresholds' but none were found",
                                                        StatusLevel.ERROR ) );
    }

    @Test
    void testFeaturefulThresholdsNotCorrelatedWithFeaturesToEvaluateProducesErrors()
    {
        Geometry featureFoo = Geometry.newBuilder()
                                      .setName( "foo" )
                                      .build();
        Geometry featureBar = Geometry.newBuilder()
                                      .setName( "bar" )
                                      .build();
        Geometry featureBaz = Geometry.newBuilder()
                                      .setName( "baz" )
                                      .build();

        GeometryTuple tupleFooBaz = GeometryTuple.newBuilder()
                                                 .setLeft( featureFoo )
                                                 .setRight( featureBaz )
                                                 .build();
        GeometryTuple tupleFooBar = GeometryTuple.newBuilder()
                                                 .setLeft( featureFoo )
                                                 .setRight( featureBar )
                                                 .build();
        GeometryTuple tupleBarBaz = GeometryTuple.newBuilder()
                                                 .setLeft( featureBar )
                                                 .setRight( featureBaz )
                                                 .build();

        Set<GeometryTuple> features = Set.of( tupleFooBaz, tupleFooBar, tupleBarBaz );

        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 1.0 ) )
                                 .build();
        wres.config.yaml.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .feature( featureFoo )
                                                                           .featureNameFrom( DatasetOrientation.RIGHT )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 2.0 ) )
                                 .build();
        wres.config.yaml.components.Threshold wrappedTwo = ThresholdBuilder.builder()
                                                                           .threshold( two )
                                                                           .feature( featureBaz )
                                                                           .featureNameFrom( DatasetOrientation.LEFT )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold three = Threshold.newBuilder()
                                   .setLeftThresholdValue( DoubleValue.of( 2.0 ) )
                                   .build();
        wres.config.yaml.components.Threshold wrappedThree = ThresholdBuilder.builder()
                                                                             .threshold( three )
                                                                             .feature( featureBar )
                                                                             .featureNameFrom( DatasetOrientation.LEFT )
                                                                             .type( ThresholdType.VALUE )
                                                                             .build();

        Threshold four = Threshold.newBuilder()
                                  .setLeftThresholdValue( DoubleValue.of( 2.0 ) )
                                  .build();
        Geometry featureQux = Geometry.newBuilder()
                                      .setName( "qux" )
                                      .build();
        wres.config.yaml.components.Threshold wrappedFour = ThresholdBuilder.builder()
                                                                            .threshold( four )
                                                                            .feature( featureQux )
                                                                            .featureNameFrom( DatasetOrientation.RIGHT )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .features( new Features( features ) )
                                            .thresholds( Set.of( wrappedOne,
                                                                 wrappedTwo,
                                                                 wrappedThree,
                                                                 wrappedFour ) )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The missing features are: [baz]",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The missing features are: [qux, foo]",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testFeaturefulThresholdsAndNoFeaturesProducesWarning()
    {
        Geometry featureFoo = Geometry.newBuilder()
                                      .setName( "foo" )
                                      .build();
        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 1.0 ) )
                                 .build();
        wres.config.yaml.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .feature( featureFoo )
                                                                           .featureNameFrom( DatasetOrientation.RIGHT )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .thresholds( Set.of( wrappedOne ) )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "the featureful thresholds cannot be "
                                                       + "validated until then",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testWrdsServicesWithoutDataTypeProducesWarnings()
    {
        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( SourceBuilder.builder()
                                                                     .sourceInterface( SourceInterface.WRDS_AHPS )
                                                                     .build() ) )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( SourceBuilder.builder()
                                                                      .sourceInterface( SourceInterface.WRDS_NWM )
                                                                      .build() ) )
                                      .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "discovered an interface of "
                                                                        + "'wrds ahps', which admits the data types",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "discovered an interface of "
                                                                        + "'wrds nwm', which admits the data types",
                                                                        StatusLevel.WARN ) ) );
    }

    @Test
    void testMinimumValidTimeLaterThanMaximumReferenceTimePlusMaximumLeadTimeProducesError()
    {
        TimeInterval referenceInterval = TimeIntervalBuilder.builder()
                                                            .minimum( Instant.parse( "2021-03-24T00:00:00Z" ) )
                                                            .maximum( Instant.parse( "2021-04-24T00:00:00Z" ) )
                                                            .build();
        TimeInterval validInterval = TimeIntervalBuilder.builder()
                                                        .minimum( Instant.parse( "2022-03-24T00:00:00Z" ) )
                                                        .maximum( Instant.parse( "2022-04-24T00:00:00Z" ) )
                                                        .build();
        LeadTimeInterval leadTimeInterval = LeadTimeIntervalBuilder.builder()
                                                                   .minimum( java.time.Duration.ZERO )
                                                                   .maximum( java.time.Duration.ofDays( 300 ) )
                                                                   .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .referenceDates( referenceInterval )
                                            .validDates( validInterval )
                                            .leadTimes( leadTimeInterval )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "The 'maximum' value of the 'reference_dates' is",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testMinimumValidTimeLaterThanMaximumReferenceTimeProducesWarning()
    {
        TimeInterval referenceInterval = TimeIntervalBuilder.builder()
                                                            .minimum( Instant.parse( "2021-03-24T00:00:00Z" ) )
                                                            .maximum( Instant.parse( "2021-04-24T00:00:00Z" ) )
                                                            .build();
        TimeInterval validInterval = TimeIntervalBuilder.builder()
                                                        .minimum( Instant.parse( "2022-03-24T00:00:00Z" ) )
                                                        .maximum( Instant.parse( "2022-04-24T00:00:00Z" ) )
                                                        .build();
        LeadTimeInterval leadTimeInterval = LeadTimeIntervalBuilder.builder()
                                                                   .minimum( java.time.Duration.ZERO )
                                                                   .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .referenceDates( referenceInterval )
                                            .validDates( validInterval )
                                            .leadTimes( leadTimeInterval )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "The 'maximum' value of the 'reference_dates' is",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testMaximumValidTimeLaterThanMinimumReferenceTimePlusMinimumLeadTimeProducesError()
    {
        TimeInterval referenceInterval = TimeIntervalBuilder.builder()
                                                            .minimum( Instant.parse( "2022-03-24T00:00:00Z" ) )
                                                            .maximum( Instant.parse( "2022-04-24T00:00:00Z" ) )
                                                            .build();
        TimeInterval validInterval = TimeIntervalBuilder.builder()
                                                        .minimum( Instant.parse( "2021-03-24T00:00:00Z" ) )
                                                        .maximum( Instant.parse( "2021-04-24T00:00:00Z" ) )
                                                        .build();
        LeadTimeInterval leadTimeInterval = LeadTimeIntervalBuilder.builder()
                                                                   .minimum( java.time.Duration.ofDays( -300 ) )
                                                                   .maximum( java.time.Duration.ZERO )
                                                                   .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .referenceDates( referenceInterval )
                                            .validDates( validInterval )
                                            .leadTimes( leadTimeInterval )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "The 'minimum' value of the 'reference_dates' is",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testMaximumValidTimeLaterThanMinimumReferenceTimeProducesWarning()
    {
        TimeInterval referenceInterval = TimeIntervalBuilder.builder()
                                                            .minimum( Instant.parse( "2022-03-24T00:00:00Z" ) )
                                                            .maximum( Instant.parse( "2022-04-24T00:00:00Z" ) )
                                                            .build();
        TimeInterval validInterval = TimeIntervalBuilder.builder()
                                                        .minimum( Instant.parse( "2021-03-24T00:00:00Z" ) )
                                                        .maximum( Instant.parse( "2021-04-24T00:00:00Z" ) )
                                                        .build();
        LeadTimeInterval leadTimeInterval = LeadTimeIntervalBuilder.builder()
                                                                   .maximum( java.time.Duration.ofDays( -300 ) )
                                                                   .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .referenceDates( referenceInterval )
                                            .validDates( validInterval )
                                            .leadTimes( leadTimeInterval )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "The 'minimum' value of the 'reference_dates' is",
                                                       StatusLevel.WARN ) );
    }

    /**
     * Redmine issue #117149.
     */

    @Test
    void testTimeScaleSeasonWithoutPeriodProducesNoErrorsOrWarnings()
    {
        wres.config.yaml.components.TimeScale timeScale =
                TimeScaleBuilder.TimeScale( TimeScale.newBuilder()
                                                     .setStartDay( 1 )
                                                     .setStartMonth( 1 )
                                                     .setEndDay( 31 )
                                                     .setEndMonth( 5 )
                                                     .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                                     .build() );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .timeScale( timeScale )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertEquals( Collections.emptyList(), events );
    }

    @Test
    void testReferencesDatesAndValidDatesDoNotOverlapButNoLeadTimesProducesWarning()
    {
        TimeInterval referenceInterval = TimeIntervalBuilder.builder()
                                                            .minimum( Instant.parse( "2021-03-24T00:00:00Z" ) )
                                                            .maximum( Instant.parse( "2021-04-24T00:00:00Z" ) )
                                                            .build();
        TimeInterval validInterval = TimeIntervalBuilder.builder()
                                                        .minimum( Instant.parse( "2022-03-24T00:00:00Z" ) )
                                                        .maximum( Instant.parse( "2022-04-24T00:00:00Z" ) )
                                                        .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .referenceDates( referenceInterval )
                                            .validDates( validInterval )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "The 'reference_dates' and 'valid_dates' do not overlap",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testPersistenceWithEnsembleDeclarationProducesError()
    {
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.defaultDataset )
                                                         .generatedBaseline( persistence )
                                                         .build();

        Dataset rightDataset = DatasetBuilder.builder( this.defaultDataset )
                                             .type( DataType.ENSEMBLE_FORECASTS )
                                             .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( rightDataset )
                                            .baseline( baseline )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "Cannot declare a 'persistence' baseline",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testClimatologyWithInvalidDatesProducesError()
    {
        GeneratedBaseline climatology = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.CLIMATOLOGY )
                                                                .maximumDate( Instant.MIN )
                                                                .minimumDate( Instant.MAX )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.defaultDataset )
                                                         .generatedBaseline( climatology )
                                                         .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .baseline( baseline )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "Discovered a climatological baseline whose "
                                                       + "'maximum_date'",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testClimatologyWithIntervalLessThan365DaysProducesError()
    {
        Instant minimum = Instant.parse( "2055-01-01T00:00:00Z" );
        Instant maximum = Instant.parse( "2055-04-01T00:00:00Z" );

        GeneratedBaseline climatology = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.CLIMATOLOGY )
                                                                .minimumDate( minimum )
                                                                .maximumDate( maximum )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.defaultDataset )
                                                         .generatedBaseline( climatology )
                                                         .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .baseline( baseline )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "Discovered a climatological baseline whose "
                                                       + "'minimum_date'",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testClimatologyWithWebServiceAndValidDatesIntervalLessThan365DaysProducesError()
    {
        Instant minimum = Instant.parse( "2055-01-01T00:00:00Z" );
        Instant maximum = Instant.parse( "2055-04-01T00:00:00Z" );

        GeneratedBaseline climatology = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.CLIMATOLOGY )
                                                                .build();
        Source webSource = SourceBuilder.builder()
                                        .uri( URI.create( "https:foo.bar/baz" ) )
                                        .build();
        Dataset webDataset = DatasetBuilder.builder()
                                           .sources( List.of( webSource ) )
                                           .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( webDataset )
                                                         .generatedBaseline( climatology )
                                                         .build();

        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( minimum )
                                                     .maximum( maximum )
                                                     .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .baseline( baseline )
                                            .validDates( validDates )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "Discovered an evaluation with a climatological "
                                                       + "baseline whose source data originates from a web service",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testClimatologyWithInvalidDataTypeProducesError()
    {
        GeneratedBaseline climatology = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.CLIMATOLOGY )
                                                                .build();
        Dataset dataset = DatasetBuilder.builder( this.defaultDataset )
                                        .type( DataType.ENSEMBLE_FORECASTS )
                                        .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .generatedBaseline( climatology )
                                                         .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .baseline( baseline )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "which requires observation-like data sources, but the "
                                                       + "declared data",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testForecastDatasetWithWebSourceDoesNotRequireValidDates()
    {
        Instant minimum = Instant.parse( "2055-01-01T00:00:00Z" );
        Instant maximum = Instant.parse( "2055-04-01T00:00:00Z" );

        Source webSource = SourceBuilder.builder()
                                        .uri( URI.create( "https://foo.bar/baz" ) )
                                        .sourceInterface( SourceInterface.WRDS_AHPS )
                                        .build();
        Dataset predicted = DatasetBuilder.builder()
                                          .sources( List.of( webSource ) )
                                          .type( DataType.SINGLE_VALUED_FORECASTS )
                                          .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( minimum )
                                                         .maximum( maximum )
                                                         .build();

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "foo" ) )
                                                             .setRight( Geometry.newBuilder()
                                                                                .setName( "foo" ) )
                                                             .build() );

        Features features = new Features( geometries );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( predicted )
                                            .referenceDates( referenceDates )
                                            .features( features )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( events.isEmpty() );
    }

    @Test
    void testSamplingUncertaintyQuantilesAreMissingProducesWarning()
    {
        SamplingUncertainty samplingUncertainty = SamplingUncertaintyBuilder.builder()
                                                                            .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .sampleUncertainty( samplingUncertainty )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "does not contain any 'quantiles', which are required",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testSamplingUncertaintyQuantilesAreInvalidProducesError()
    {
        SamplingUncertainty samplingUncertainty = SamplingUncertaintyBuilder.builder()
                                                                            .quantiles( new TreeSet<>( Set.of( -0.1,
                                                                                                               0.0,
                                                                                                               1.0,
                                                                                                               1.5 ) ) )
                                                                            .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .sampleUncertainty( samplingUncertainty )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "'sampling_uncertainty' must be greater than 0.0 and "
                                                       + "less than 1.0",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testSamplingUncertaintySampleSizeIsSmallProducesWarning()
    {
        SamplingUncertainty samplingUncertainty = SamplingUncertaintyBuilder.builder()
                                                                            .sampleSize( 100 )
                                                                            .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .sampleUncertainty( samplingUncertainty )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "This may lead to inaccurate estimates of the sampling "
                                                       + "uncertainty",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testSamplingUncertaintyWithoutCrossPairingProducesWarning()
    {
        SamplingUncertainty samplingUncertainty = SamplingUncertaintyBuilder.builder()
                                                                            .sampleSize( 1000 )
                                                                            .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.defaultDataset )
                                                         .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .baseline( baseline )
                                            .sampleUncertainty( samplingUncertainty )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "This is allowed, but can lead to the nominal value",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testSamplingUncertaintySampleSizeIsTooLargeProducesError()
    {
        SamplingUncertainty samplingUncertainty = SamplingUncertaintyBuilder.builder()
                                                                            .sampleSize( 100_001 )
                                                                            .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .sampleUncertainty( samplingUncertainty )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "is larger than the reasonable maximum",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testSummaryStatisticsAcrossFeaturesWithoutFeatureDeclarationProducesError()
    {
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                            .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                            .build();

        Set<SummaryStatistic> summaryStatistics = Set.of( summaryStatistic );

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "across geographic features, but no features were "
                                                       + "declared for evaluation",
                                                       StatusLevel.WARN ) );

    }

    @Test
    void testSummaryStatisticsQuantilesWithoutMedianAndWithDiagramMetricAndGraphicsProducesWarning()
    {
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                            .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                            .setProbability( 0.4 )
                                                            .build();

        Set<SummaryStatistic> summaryStatistics = Set.of( summaryStatistic );

        Metric metric = new Metric( MetricConstants.QUANTILE_QUANTILE_DIAGRAM, null );
        Set<Metric> metrics = Set.of( metric );
        Formats formats = new Formats( Outputs.newBuilder()
                                              .setPng( Outputs.PngFormat.newBuilder()
                                                                        .build() )
                                              .build() );

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .metrics( metrics )
                                            .summaryStatistics( summaryStatistics )
                                            .formats( formats )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "alongside diagram metrics and graphics formats",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testSummaryStatisticsAcrossFeatureGroupsWithoutFeatureGroupsProducesError()
    {
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.FEATURE_GROUP )
                                                            .setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                                            .build();

        Set<SummaryStatistic> summaryStatistics = Set.of( summaryStatistic );

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "no feature groups with multiple features were declared",
                                                       StatusLevel.ERROR ) );

    }

    @Test
    void testSummaryStatisticsAcrossFeatureGroupsPassesValidationWhenFeatureServiceGroupsAreDeclared()
    {
        // #124265-44
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.FEATURE_GROUP )
                                                            .setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                                            .build();

        Set<SummaryStatistic> summaryStatistics = Set.of( summaryStatistic );

        FeatureServiceGroup group = new FeatureServiceGroup( "a", "b", true );
        FeatureService featureService = new FeatureService( URI.create( "http://foo.bar" ), Set.of( group ) );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .featureService( featureService )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertFalse( DeclarationValidatorTest.contains( events,
                                                        "no 'feature_groups' were declared",
                                                        StatusLevel.ERROR ) );

    }

    @Test
    void testSummaryStatisticsAcrossFeatureGroupsFailsValidationWhenFeatureServiceGroupsAreSingletons()
    {
        // #124265-44
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.FEATURE_GROUP )
                                                            .setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                                            .build();

        Set<SummaryStatistic> summaryStatistics = Set.of( summaryStatistic );

        // Feature service group whose members are singletons
        FeatureServiceGroup group = new FeatureServiceGroup( "a", "b", false );
        FeatureService featureService = new FeatureService( URI.create( "http://foo.bar" ), Set.of( group ) );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .featureService( featureService )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "no feature groups with multiple features were declared",
                                                       StatusLevel.ERROR ) );

    }

    @Test
    void testSamplingUncertaintyAndFeatureGroupsWithUnexpectedCrossPairingOptionProducesWarning()
    {
        SamplingUncertainty samplingUncertainty = SamplingUncertaintyBuilder.builder()
                                                                            .sampleSize( 1000 )
                                                                            .build();

        FeatureServiceGroup group = new FeatureServiceGroup( "a", "b", true );
        FeatureService featureService = new FeatureService( URI.create( "http://foo.bar" ), Set.of( group ) );
        CrossPair crossPair = new CrossPair( CrossPairMethod.FUZZY, CrossPairScope.WITHIN_FEATURES );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.defaultDataset )
                                            .right( this.defaultDataset )
                                            .baseline( new BaselineDataset( this.defaultDataset, null, null ) )
                                            .featureService( featureService )
                                            .sampleUncertainty( samplingUncertainty )
                                            .crossPair( crossPair )
                                            .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "please consider declaring these two cross-pairing "
                                                       + "options",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testInvalidDataSourceUrisProduceErrors()
    {
        // Null sentinels for source URIs that could not be parsed
        Source source = SourceBuilder.builder()
                                     .uri( null )
                                     .build();
        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( source ) )
                                        .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .build();

        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( dataset )
                                                            .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
                                                                        .baseline( baseline )
                                                                        .covariates( List.of( covariate ) )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "at the following positions in "
                                                                                + "the 'observed' data source were "
                                                                                + "invalid",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "at the following positions in "
                                                                                + "the 'predicted' data source were "
                                                                                + "invalid",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "at the following positions in "
                                                                                + "the 'baseline' data source were "
                                                                                + "invalid",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "at the following positions in "
                                                                                + "a 'covariate' data source were "
                                                                                + "invalid",
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testInconsistentGraphicsOrientationAndPoolingDeclarationProducesError() throws IOException  // NOSONAR
    {
        // #57969-86
        String evaluation = """
                observed: foo/data/DOSC1_QIN.xml
                predicted: bar/DOSC1_SQIN.xml
                valid_dates:
                  minimum: 1988-10-01T00:00:00Z
                  maximum: 2000-01-01T00:00:00Z
                valid_date_pools:
                  period: 2400
                  frequency: 2400
                  unit: hours
                output_formats:
                  - format: png
                    orientation: lead threshold
                """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "Please correct the 'orientation'",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testInvalidDeclarationStringProducesSchemaValidationError() throws IOException  // NOSONAR
    {
        // #57969-86
        String evaluation = """
                label: NWM AnA Test
                observed:
                  label: USGS NWIS Instantaneous Streamflow Observations
                  sources:
                  - interface: usgs nwis
                    uri: https://nwis.waterservices.usgs.gov/nwis/iv
                  variable: '00060'
                predicted:
                  label: WRES NWM AnA
                  sources:
                  - interface: wrds nwm
                    uri: https://***REMOVED***.***REMOVED***.***REMOVED***/api/nwm2.1/v2.0/ops/analysis_assim/
                    parameters:
                      unassimilated: 'true'
                  variable: streamflow
                unit: ft3/s
                valid_dates:
                  minimum: '2023-09-04T00:00:00Z'
                  maximum: '2023-09-14T19:43:24Z'
                analysis_durations:
                  minimum_exclusive: -2
                  maximum: 0
                  unit: hours
                """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "is not defined in the schema",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testIncorrectlySpacedYamlProducesStatusEventErrorNotException() throws IOException
    {
        // #120626: invalid spacing of minimum_exclusive
        String evaluation = """
                label: NWM AnA Test
                observed:
                  label: USGS NWIS Instantaneous Streamflow Observations
                  sources:
                  - interface: usgs nwis
                    uri: https://nwis.waterservices.usgs.gov/nwis/iv
                  variable: '00060'
                predicted:
                  label: WRES NWM AnA
                  sources:
                  - interface: wrds nwm
                    uri: https://foo/api/nwm2.1/v2.0/ops/analysis_assim/
                    parameters:
                      unassimilated: 'true'
                  variable: streamflow
                unit: ft3/s
                valid_dates:
                  minimum: '2023-09-04T00:00:00Z'
                  maximum: '2023-09-14T19:43:24Z'
                analysis_times:
                    minimum: -2
                  maximum: 0
                  unit: hours
                feature_service:
                  uri: https://foo/api/location/v3.0/metadata
                  """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "invalid YAML",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testMalformedYamlProducesStatusEventErrorNotException() throws IOException
    {
        // #124676: malformed yaml
        String evaluation = """
                web demo
                observed: /mnt/wres_share/systests/data/DRRC2QINE.xml
                predicted:
                  label: HEFS
                  sources: /mnt/wres_share/systests/data/drrc2ForecastsOneMonth/
                unit: m3/s
                lead_times:
                  minimum: 0
                  maximum: 48
                  unit: hours
                probability_thresholds:
                  values: [0.002, 0.01, 0.1, 0.9, 0.99, 0.998]
                  operator: greater equal
                metrics:
                  - quantile quantile diagram
                  - rank histogram
                  - relative operating characteristic diagram
                  - box plot of errors by forecast value
                  - root mean square error
                  - sample size
                  - mean absolute error
                  - box plot of errors by observed value
                  - reliability diagram
                  """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "invalid YAML",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testDuplicateThresholdsProducesErrorOnUniqueConstraintViolation() throws IOException
    {
        // #120047: invalid spacing of minimum_exclusive
        String evaluation = """
                observed: foo.csv
                predicted: bar.csv
                classifier_thresholds:
                  values: [0.05, 0.05, 0.1, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]
                  operator: equal
                  apply_to: any predicted
                """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "the items in the array must be unique",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testDuplicateMetricNamesWithDifferentMetricParametersProducesWarning() throws IOException
    {
        // #120345: duplication of metric names
        String evaluation = """
                observed: observations.csv
                predicted: predictions.csv
                metrics:
                  - name: time to peak error
                    summary_statistics:
                      - median
                      - minimum
                      - maximum
                      - mean absolute
                      - mean
                      - standard deviation
                  - name: time to peak error
                """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "duplicate metrics",
                                                       StatusLevel.WARN ) );
    }

    @Test
    void testTimeScaleElementsThatAreDependentRequiredAndMissingProducesErrors() throws IOException
    {
        // #120552: timescale period with missing unit, plus some date elements with missing dependents
        String evaluation = """
                observed: observations.csv
                predicted: predictions.csv
                time_scale:
                  function: maximum
                  period: 24         # Requires units
                  minimum_day: 1     # Requires minimum_month
                  maximum_month: 2   # Requires maximum_day
                """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "time_scale: has a missing property "
                                                                        + "\"unit\"",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "time_scale: has a missing property "
                                                                        + "\"minimum_month\"",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "time_scale: has a missing property "
                                                                        + "\"maximum_day\"",
                                                                        StatusLevel.ERROR ) ) );
    }

    @Test
    void testMissingObservedDatasetProducesErrorAndDoesNotProduceNullPointerException() throws IOException
    {
        // #126326
        String evaluation = """
                label: web demo
                observed:
                predicted:
                  label: HEFS
                  sources: /mnt/wres_share/systests/data/drrc2ForecastsOneMonth/
                unit: m3/s
                lead_times:
                  minimum: 0
                  maximum: 48
                  unit: hours
                probability_thresholds:
                  values: [0.002, 0.01, 0.1, 0.9, 0.99, 0.998]
                  operator: greater equal
                metrics:
                  - quantile quantile diagram
                """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "does not contain an 'observed'",
                                                       StatusLevel.ERROR ) );
    }

    @Test
    void testValidateXmlDeclarationProducesWarning() throws IOException
    {
        // #121176
        String evaluation = """
                <project name="RFC_AHPS_Flow">
                  <inputs>
                    <left label="USGS NWIS Streamflow Observations">
                      <type>observations</type>
                      <source interface="usgs_nwis">https://nwis.waterservices.usgs.gov/nwis/iv</source>
                      <variable>00060</variable>
                    </left>
                    <right label="RFC_AHPS_Flow">
                      <type>single valued forecasts</type>
                      <source interface="wrds_ahps">http://WRDS/api/rfc_forecast/v2.0/forecast/streamflow</source>
                      <variable>QR</variable>
                    </right>
                  </inputs>
                </project>
                  """;

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( evaluation );

        assertTrue( DeclarationValidatorTest.contains( events,
                                                       "XML declaration language has been deprecated",
                                                       StatusLevel.WARN ) );
    }

    /**
     * @param events the events to check
     * @param message the message sequence that should appear in one or more messages
     * @param level the level of the event
     * @return whether the events have one or more instance with the prescribed level and characters
     */
    private static boolean contains( List<EvaluationStatusEvent> events, String message, StatusLevel level )
    {
        return events.stream()
                     .anyMatch( next -> next.getStatusLevel() == level && next.getEventMessage()
                                                                              .contains( message ) );
    }
}
