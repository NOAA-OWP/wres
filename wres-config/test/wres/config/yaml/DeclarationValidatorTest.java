package wres.config.yaml;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimeScaleLenience;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.Pool;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link DeclarationValidator}.
 * @author James Brown
 */
class DeclarationValidatorTest
{
    @Test
    void testTypesAreDefinedResultsInErrors()
    {
        Dataset dataset = DatasetBuilder.builder()
                                        .build();
        BaselineDataset baselineDataset = BaselineDatasetBuilder.builder()
                                                                .dataset( dataset )
                                                                .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( dataset )
                                                                        .right( dataset )
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
                                     .api( SourceInterface.USGS_NWIS )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .api( SourceInterface.WRDS_NWM )
                                            .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( source, anotherSource ) )
                                        .type( DataType.OBSERVATIONS )
                                        .build();
        BaselineDataset baseline = new BaselineDataset( dataset, null );
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
                                     .api( SourceInterface.USGS_NWIS )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .api( SourceInterface.WRDS_NWM )
                                            .build();
        Source yetAnotherSource = SourceBuilder.builder()
                                               .api( SourceInterface.WRDS_AHPS )
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
        BaselineDataset baseline = new BaselineDataset( baselineInner, null );
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
                                     .api( SourceInterface.USGS_NWIS )
                                     .build();
        Source anotherSource = SourceBuilder.builder()
                                            .api( SourceInterface.WRDS_AHPS )
                                            .build();
        Source yetAnotherSource = SourceBuilder.builder()
                                               .api( SourceInterface.WRDS_AHPS )
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
        BaselineDataset baseline = new BaselineDataset( baselineInner, null );
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
                                     .api( SourceInterface.NWM_SHORT_RANGE_CHANNEL_RT_CONUS )
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
        Source source = SourceBuilder.builder()
                                     .build();

        Dataset left = DatasetBuilder.builder()
                                     .sources( List.of( source ) )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .sources( List.of( source ) )
                                      .build();

        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( left )
                                              .right( right )
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
