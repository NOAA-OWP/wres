package wres.datamodel.baselines;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link ClimatologyGenerator}.
 * @author James Brown
 */
class ClimatologyGeneratorTest
{
    // Times used
    private static final Instant T1980_01_01T12_00_00Z = Instant.parse( "1980-01-01T12:00:00Z" );
    private static final Instant T1980_01_01T18_00_00Z = Instant.parse( "1980-01-01T18:00:00Z" );
    private static final Instant T1980_01_02T00_00_00Z = Instant.parse( "1980-01-02T00:00:00Z" );
    private static final Instant T1981_01_01T06_00_00Z = Instant.parse( "1981-01-01T06:00:00Z" );
    private static final Instant T1981_01_01T12_00_00Z = Instant.parse( "1981-01-01T12:00:00Z" );
    private static final Instant T1981_01_01T18_00_00Z = Instant.parse( "1981-01-01T18:00:00Z" );
    private static final Instant T1981_01_02T00_00_00Z = Instant.parse( "1981-01-02T00:00:00Z" );
    private static final Instant T1982_01_01T12_00_00Z = Instant.parse( "1982-01-01T12:00:00Z" );
    private static final Instant T1982_01_01T18_00_00Z = Instant.parse( "1982-01-01T18:00:00Z" );
    private static final Instant T1982_01_02T00_00_00Z = Instant.parse( "1982-01-02T00:00:00Z" );
    private static final Instant T1983_01_01T06_00_00Z = Instant.parse( "1983-01-01T06:00:00Z" );
    private static final Instant T1983_01_01T12_00_00Z = Instant.parse( "1983-01-01T12:00:00Z" );
    private static final Instant T1983_01_01T18_00_00Z = Instant.parse( "1983-01-01T18:00:00Z" );
    private static final Instant T1983_01_02T00_00_00Z = Instant.parse( "1983-01-02T00:00:00Z" );

    /** A climatological time-series. */
    private TimeSeries<Double> climatology;
    /** A climatology generator. */
    private ClimatologyGenerator generator;
    /** variable name. */
    private static final String STREAMFLOW = "STREAMFLOW";
    /** Measurement unit. */
    private static final String CMS = "CMS";
    /** feature name. */
    private static final Feature FAKE = Feature.of( MessageFactory.getGeometry( "FAKE" ) );

    @BeforeEach
    public void runBeforeEachTest()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TimeScaleOuter.of( Duration.ofSeconds( 1 ),
                                                                                TimeScale.TimeScaleFunction.MEAN ),
                                                             STREAMFLOW,
                                                             FAKE,
                                                             CMS );

        this.climatology =
                new TimeSeries.Builder<Double>().addEvent( Event.of( T1980_01_01T12_00_00Z, 313.0 ) )
                                                .addEvent( Event.of( T1980_01_01T18_00_00Z, 317.0 ) )
                                                .addEvent( Event.of( T1980_01_02T00_00_00Z, 331.0 ) )
                                                .addEvent( Event.of( T1981_01_01T12_00_00Z, 347.0 ) )
                                                .addEvent( Event.of( T1981_01_01T18_00_00Z, 349.0 ) )
                                                .addEvent( Event.of( T1981_01_02T00_00_00Z, 353.0 ) )
                                                .addEvent( Event.of( T1982_01_01T12_00_00Z, 359.0 ) )
                                                .addEvent( Event.of( T1982_01_01T18_00_00Z, 367.0 ) )
                                                .addEvent( Event.of( T1982_01_02T00_00_00Z, 373.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        this.generator = ClimatologyGenerator.of( () -> Stream.of( this.climatology ),
                                                  TimeSeriesOfDoubleUpscaler.of(),
                                                  CMS );
    }

    @Test
    void testApply()
    {
        // Forecast time scale
        TimeScaleOuter existingTimeScale =
                TimeScaleOuter.of( Duration.ofSeconds( 1 ), TimeScale.TimeScaleFunction.MEAN );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.UNKNOWN,
                                                                        T1983_01_01T06_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE,
                                                                CMS );

        // Forecast, which does not overlap with climatology
        TimeSeries<Ensemble> forecast =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T12_00_00Z, Ensemble.of( 1.0 ) ) )
                                                  .addEvent( Event.of( T1983_01_01T18_00_00Z, Ensemble.of( 2.0 ) ) )
                                                  .addEvent( Event.of( T1983_01_02T00_00_00Z, Ensemble.of( 3.0 ) ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        TimeSeries<Ensemble> actual = this.generator.apply( forecast );

        Ensemble.Labels labelStrings = Ensemble.Labels.of( "1980", "1981", "1982" );
        double[] one = new double[] { 313.0, 347.0, 359.0 };
        Ensemble first = Ensemble.of( one, labelStrings );
        double[] two = new double[] { 317.0, 349.0, 367.0 };
        Ensemble second = Ensemble.of( two, labelStrings );
        double[] three = new double[] { 331.0, 353.0, 373.0 };
        Ensemble third = Ensemble.of( three, labelStrings );

        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T12_00_00Z, first ) )
                                                  .addEvent( Event.of( T1983_01_01T18_00_00Z, second ) )
                                                  .addEvent( Event.of( T1983_01_02T00_00_00Z, third ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithUnitChange()
    {
        // Create template metadata with a different unit than the source metadata
        TimeScaleOuter existingTimeScale =
                TimeScaleOuter.of( Duration.ofSeconds( 1 ), TimeScale.TimeScaleFunction.MEAN );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.UNKNOWN,
                                                                        T1983_01_01T06_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE,
                                                                "CFS" );

        // Forecast, which does not overlap with climatology
        TimeSeries<Ensemble> forecast =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T12_00_00Z, Ensemble.of( 1.0 ) ) )
                                                  .addEvent( Event.of( T1983_01_01T18_00_00Z, Ensemble.of( 2.0 ) ) )
                                                  .addEvent( Event.of( T1983_01_02T00_00_00Z, Ensemble.of( 3.0 ) ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        TimeSeries<Ensemble> actual = this.generator.apply( forecast );

        Ensemble.Labels labelStrings = Ensemble.Labels.of( "1980", "1981", "1982" );
        double[] one = new double[] { 313.0, 347.0, 359.0 };
        Ensemble first = Ensemble.of( one, labelStrings );
        double[] two = new double[] { 317.0, 349.0, 367.0 };
        Ensemble second = Ensemble.of( two, labelStrings );
        double[] three = new double[] { 331.0, 353.0, 373.0 };
        Ensemble third = Ensemble.of( three, labelStrings );

        TimeSeriesMetadata expectedMetadata = new TimeSeriesMetadata.Builder( metadataOne ).setUnit( CMS )
                                                                                           .build();
        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T12_00_00Z, first ) )
                                                  .addEvent( Event.of( T1983_01_01T18_00_00Z, second ) )
                                                  .addEvent( Event.of( T1983_01_02T00_00_00Z, third ) )
                                                  .setMetadata( expectedMetadata )
                                                  .build();

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithOverlappingSource()
    {
        // Forecast time scale
        TimeScaleOuter existingTimeScale =
                TimeScaleOuter.of( Duration.ofSeconds( 1 ), TimeScale.TimeScaleFunction.MEAN );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.UNKNOWN,
                                                                        T1981_01_01T06_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE,
                                                                CMS );

        // Forecast, which does not overlap with climatology
        TimeSeries<Ensemble> forecast =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1981_01_01T12_00_00Z, Ensemble.of( 1.0 ) ) )
                                                  .addEvent( Event.of( T1981_01_01T18_00_00Z, Ensemble.of( 2.0 ) ) )
                                                  .addEvent( Event.of( T1981_01_02T00_00_00Z, Ensemble.of( 3.0 ) ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        TimeSeries<Ensemble> actual = this.generator.apply( forecast );

        Ensemble.Labels labelStrings = Ensemble.Labels.of( "1980", "1982" );
        double[] one = new double[] { 313.0, 359.0 };
        Ensemble first = Ensemble.of( one, labelStrings );
        double[] two = new double[] { 317.0, 367.0 };
        Ensemble second = Ensemble.of( two, labelStrings );
        double[] three = new double[] { 331.0, 373.0 };
        Ensemble third = Ensemble.of( three, labelStrings );

        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1981_01_01T12_00_00Z, first ) )
                                                  .addEvent( Event.of( T1981_01_01T18_00_00Z, second ) )
                                                  .addEvent( Event.of( T1981_01_02T00_00_00Z, third ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithExplicitDateInterval()
    {
        // Forecast time scale
        TimeScaleOuter existingTimeScale =
                TimeScaleOuter.of( Duration.ofSeconds( 1 ), TimeScale.TimeScaleFunction.MEAN );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.UNKNOWN,
                                                                        T1983_01_01T06_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE,
                                                                CMS );

        // Forecast, which does not overlap with climatology
        TimeSeries<Ensemble> forecast =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T12_00_00Z, Ensemble.of( 1.0 ) ) )
                                                  .addEvent( Event.of( T1983_01_01T18_00_00Z, Ensemble.of( 2.0 ) ) )
                                                  .addEvent( Event.of( T1983_01_02T00_00_00Z, Ensemble.of( 3.0 ) ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        // Generator with explicit interval
        GeneratedBaseline generated = GeneratedBaselineBuilder.builder()
                                                              .minimumDate( T1981_01_01T06_00_00Z )
                                                              .maximumDate( T1983_01_02T00_00_00Z )
                                                              .build();

        ClimatologyGenerator restricted = ClimatologyGenerator.of( () -> Stream.of( this.climatology ),
                                                                   TimeSeriesOfDoubleUpscaler.of(),
                                                                   CMS,
                                                                   generated );

        TimeSeries<Ensemble> actual = restricted.apply( forecast );

        Ensemble.Labels labelStrings = Ensemble.Labels.of( "1981", "1982" );
        double[] one = new double[] { 347.0, 359.0 };
        Ensemble first = Ensemble.of( one, labelStrings );
        double[] two = new double[] { 349.0, 367.0 };
        Ensemble second = Ensemble.of( two, labelStrings );
        double[] three = new double[] { 353.0, 373.0 };
        Ensemble third = Ensemble.of( three, labelStrings );

        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T12_00_00Z, first ) )
                                                  .addEvent( Event.of( T1983_01_01T18_00_00Z, second ) )
                                                  .addEvent( Event.of( T1983_01_02T00_00_00Z, third ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithUpscaling()
    {
        // Forecast time scale
        TimeScaleOuter existingTimeScale =
                TimeScaleOuter.of( Duration.ofHours( 12 ), TimeScale.TimeScaleFunction.MEAN );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.UNKNOWN,
                                                                        T1983_01_01T06_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE,
                                                                CMS );

        // Forecast, which does not overlap with climatology
        TimeSeries<Ensemble> forecast =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T18_00_00Z, Ensemble.of( 1.0 ) ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        TimeSeries<Ensemble> actual = this.generator.apply( forecast );

        Ensemble.Labels labelStrings = Ensemble.Labels.of( "1980", "1981", "1982" );
        double[] one = new double[] { 315.0, 348.0, 363.0 };
        Ensemble first = Ensemble.of( one, labelStrings );

        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T18_00_00Z, first ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        assertEquals( expected, actual );
    }

    @Test
    void testConstructionThrowsExpectedExceptionForInvalidInterval()
    {
        Stream<TimeSeries<Double>> data = Stream.of( this.climatology );
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        GeneratedBaseline generated = GeneratedBaselineBuilder.builder()
                                                              .minimumDate( T1983_01_01T06_00_00Z )
                                                              .maximumDate( T1981_01_02T00_00_00Z )
                                                              .build();
        BaselineGeneratorException actual =
                assertThrows( BaselineGeneratorException.class,
                              () -> ClimatologyGenerator.of( () -> data,
                                                             upscaler,
                                                             CMS,
                                                             generated ) );

        assertTrue( actual.getMessage()
                          .contains( "The climatology period is invalid" ) );
    }

    @Test
    void testConstructionThrowsExpectedExceptionForNoSourceTimeSeries()
    {
        Stream<TimeSeries<Double>> data = Stream.of();
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        BaselineGeneratorException actual =
                assertThrows( BaselineGeneratorException.class,
                              () -> ClimatologyGenerator.of( () -> data,
                                                             upscaler,
                                                             CMS ) );

        assertTrue( actual.getMessage().contains( "Cannot create a climatology time-series without one or more" ) );
    }

    @Test
    void testConstructionThrowsExpectedExceptionForEmptySourceTimeSeries()
    {
        Stream<TimeSeries<Double>> data = Stream.of( TimeSeries.of( TimeSeriesMetadata.of( Map.of(),
                                                                                           null,
                                                                                           STREAMFLOW,
                                                                                           FAKE,
                                                                                           CMS ) ) );
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        BaselineGeneratorException actual =
                assertThrows( BaselineGeneratorException.class,
                              () -> ClimatologyGenerator.of( () -> data,
                                                             upscaler,
                                                             CMS ) );

        assertTrue( actual.getMessage()
                          .contains( "Cannot create a climatology time-series without one or more" ) );
    }

    @Test
    void testConstructionThrowsExpectedExceptionForSourceTimeSeriesWithT0ReferenceTime()
    {
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                        T1983_01_01T06_00_00Z ),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE,
                                                                CMS );

        // Forecast, which does not overlap with climatology
        TimeSeries<Double> forecast =
                new TimeSeries.Builder<Double>().addEvent( Event.of( T1983_01_01T18_00_00Z, 1.0 ) )
                                                .setMetadata( metadataOne )
                                                .build();

        Stream<TimeSeries<Double>> data = Stream.of( forecast );
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        BaselineGeneratorException actual =
                assertThrows( BaselineGeneratorException.class,
                              () -> ClimatologyGenerator.of( () -> data,
                                                             upscaler,
                                                             CMS ) );

        assertTrue( actual.getMessage()
                          .contains( "discovered one or more time-series that contained a reference time" ) );
    }

    @Test
    void testApplyThrowsExpectedExceptionForTemplateTimeSeriesWithUnexpectedFeature()
    {
        Feature foo = Feature.of( MessageFactory.getGeometry( "FOO" ) );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                        T1983_01_01T06_00_00Z ),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                foo,
                                                                CMS );

        // Forecast, which does not overlap with climatology
        TimeSeries<Ensemble> forecast =
                new TimeSeries.Builder<Ensemble>().addEvent( Event.of( T1983_01_01T18_00_00Z, Ensemble.of( 1.0 ) ) )
                                                  .setMetadata( metadataOne )
                                                  .build();

        BaselineGeneratorException actual =
                assertThrows( BaselineGeneratorException.class,
                              () -> this.generator.apply( forecast ) );

        assertTrue( actual.getMessage()
                          .contains( "Source time-series were only available for features" ) );
    }

}
