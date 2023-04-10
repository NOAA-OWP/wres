package wres.config.yaml;

import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import com.google.protobuf.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisDurations;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.UnitAlias;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
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
    void runBeforeEach()
    {
        Source source = SourceBuilder.builder()
                                     .build();

        this.defaultDataset = DatasetBuilder.builder()
                                            .sources( List.of( source ) )
                                            .build();
    }

    @Test
    void testTypesAreNotDefinedResultsInErrors()
    {
        BaselineDataset baselineDataset = BaselineDatasetBuilder.builder()
                                                                .dataset( this.defaultDataset )
                                                                .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .baseline( baselineDataset )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "The data type was undefined "
                                                                                + "for the observed",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The data type was undefined "
                                                                                + "for the predicted",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The data type was undefined "
                                                                                + "for the baseline",
                                                                        StatusLevel.ERROR ) )
        );
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
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
                                                                        .baseline( baseline )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'observed' data "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'observed' data "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'predicted' data "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'predicted' data "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'baseline' data "
                                                                                + "with an interface shorthand of usgs "
                                                                                + "nwis, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "for the 'baseline' data "
                                                                                + "with an interface shorthand of wrds "
                                                                                + "nwm, which requires the 'variable'",
                                                                        StatusLevel.ERROR ) )

        );
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
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .baseline( baseline )
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
                                                                        StatusLevel.ERROR ) )
        );
    }

    @Test
    void testTimeZoneForSourceResultsInWarning()
    {
        Source source = SourceBuilder.builder()
                                     .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                            .build();
        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( anotherSource ) )
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "Discovered one or more "
                                                                                + "'observed' data sources for which a "
                                                                                + "time zone was declared",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "Discovered one or more "
                                                                                + "'predicted' data sources for which "
                                                                                + "a time zone was declared",
                                                                        StatusLevel.WARN ) )
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
                                              .referenceDatePools( new TimePools( 1,
                                                                                  null,
                                                                                  ChronoUnit.HOURS ) )
                                              .leadTimes( new LeadTimeInterval( 0, 1, ChronoUnit.HOURS ) )
                                              .leadTimePools( new TimePools( 1, null, ChronoUnit.HOURS ) )
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
    void testSourceTimeScaleIsValidResultsinError()
    {
        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .setStartDay( 1 )
                                            .setEndDay( 1 )
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );
        Source source = SourceBuilder.builder()
                                     .timeScale( timeScale )
                                     .build();

        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( source ) )
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
                                                                        "The time scale period must be "
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
    void testEvaluationTimeScaleIsConsistentWithSourceTimeScalesResultsInErrors()
    {
        TimeScale timeScaleInnerSource = TimeScale.newBuilder()
                                                  .setPeriod( Duration.newBuilder()
                                                                      .setSeconds( 23 )
                                                                      .build() )
                                                  .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                                  .build();
        wres.config.yaml.components.TimeScale timeScaleSource =
                new wres.config.yaml.components.TimeScale( timeScaleInnerSource );

        Source source = SourceBuilder.builder()
                                     .timeScale( timeScaleSource )
                                     .build();

        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( source ) )
                                      .build();

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
                                              .timeScale( timeScale )
                                              .build();

        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertAll( () -> assertTrue( DeclarationValidatorTest.contains( events, "cannot be instantaneous",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "is smaller than the "
                                                                                + "evaluation 'time_scale'",
                                                                        StatusLevel.ERROR ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "is not exactly divisible",
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
        LeadTimeInterval leadInterval = new LeadTimeInterval( 3, 1, ChronoUnit.HOURS );
        AnalysisDurations analysisDurations = new AnalysisDurations( 1, 0, ChronoUnit.HOURS );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .referenceDates( interval )
                                                                        .validDates( interval )
                                                                        .leadTimes( leadInterval )
                                                                        .analysisDurations( analysisDurations )
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
                   () -> assertTrue( DeclarationValidatorTest.contains( events, "The 'analysis_durations' "
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
        TimePools timePool = new TimePools( 1, 1, ChronoUnit.HOURS );
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
        TimePools timePool = new TimePools( 1, null, ChronoUnit.HOURS );
        TimeInterval interval = new TimeInterval( Instant.parse( "2047-01-01T00:00:00Z" ),
                                                  Instant.parse( "2047-01-01T00:01:00Z" ) );
        LeadTimeInterval leadInterval = new LeadTimeInterval( 1, 2, ChronoUnit.MINUTES );
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
    void testInvalidSpatialMaskResultsInError()
    {
        SpatialMask mask = new SpatialMask( null, "foo_invalid_mask", 0L );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.defaultDataset )
                                                                        .right( this.defaultDataset )
                                                                        .spatialMask( mask )
                                                                        .build();
        List<EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        assertTrue( DeclarationValidatorTest.contains( events, "The 'wkt' string associated with the "
                                                               + "'spatial_mask' could not be parsed into a geometry",
                                                       StatusLevel.ERROR ) );
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
                                                               + "features for a baseline dataset but no baseline "
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
        TimePools timePool = new TimePools( 3, 1, ChronoUnit.HOURS );
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
                                                                        + "require either 'probability_thresholds' or "
                                                                        + "'value_thresholds' but none were found",
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
                                                                        "The declaration requested 'csv' "
                                                                        + "format, which has been marked deprecated",
                                                                        StatusLevel.WARN ) ),
                   () -> assertTrue( DeclarationValidatorTest.contains( events,
                                                                        "The declaration requested 'netcdf' "
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
                                                                        StatusLevel.WARN ) )
        );
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
