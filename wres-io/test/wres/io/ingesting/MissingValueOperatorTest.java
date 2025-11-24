package wres.io.ingesting;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import wres.config.components.DatasetBuilder;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.datamodel.types.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.TimeSeriesTuple;
import wres.datamodel.time.TimeSeries.Builder;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link MissingValueOperator}.
 * @author James Brown
 */

class MissingValueOperatorTest
{
    @Test
    void testApplyToSingleValuedTimeSeries()
    {
        // Six event times, PT1H apart
        Instant first = Instant.parse( "2079-12-03T00:00:00Z" );
        Instant second = Instant.parse( "2079-12-03T01:00:00Z" );
        Instant third = Instant.parse( "2079-12-03T02:00:00Z" );
        Instant fourth = Instant.parse( "2079-12-03T04:00:00Z" );
        Instant fifth = Instant.parse( "2079-12-03T05:00:00Z" );
        Instant sixth = Instant.parse( "2079-12-03T06:00:00Z" );

        // Six events with two missings
        Event<Double> one = Event.of( first, 1.0 );
        Event<Double> two = Event.of( second, -999.0 );
        Event<Double> three = Event.of( third, 2.0 );
        Event<Double> four = Event.of( fourth, 3.0 );
        Event<Double> five = Event.of( fifth, -998.0 );
        Event<Double> six = Event.of( sixth, 5.0 );

        // Time scale of the event values: instantaneous
        TimeScaleOuter existingScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.MEAN );
        Feature feature = Feature.of( Geometry.getDefaultInstance() );
        TimeSeriesMetadata meta = TimeSeriesMetadata.of( Map.of(),
                                                         existingScale,
                                                         "foo",
                                                         feature,
                                                         "bar" );

        TimeSeries<Double> timeSeries = new Builder<Double>().addEvent( one )
                                                             .addEvent( two )
                                                             .addEvent( three )
                                                             .addEvent( four )
                                                             .addEvent( five )
                                                             .addEvent( six )
                                                             .setMetadata( meta )
                                                             .build();

        // Missings declaration
        Source source = SourceBuilder.builder()
                                     .missingValue( List.of( -999.0, -998.0 ) )
                                     .build();
        TimeSeriesTuple tuple =
                TimeSeriesTuple.ofSingleValued( timeSeries,
                                                DataSource.builder()
                                                          .disposition( DataDisposition.CSV_WRES )
                                                          .source( source )
                                                          .context( DatasetBuilder.builder()
                                                                                  .sources( List.of( source ) )
                                                                                  .build() )
                                                          .links( List.of() )
                                                          .uri( URI.create( "https://fake.uri" ) )
                                                          .build() );

        MissingValueOperator operator = MissingValueOperator.of();

        TimeSeriesTuple actual = operator.apply( Stream.of( tuple ) )
                                         .findFirst()
                                         .orElseThrow();

        TimeSeries<Double> actualSeries = actual.getSingleValuedTimeSeries();

        // Create the expected series
        Event<Double> twoExpected = Event.of( second, MissingValues.DOUBLE );
        Event<Double> fiveExpected = Event.of( fifth, MissingValues.DOUBLE );

        TimeSeries<Double> expectedSeries = new Builder<Double>().addEvent( one )
                                                                 .addEvent( twoExpected )
                                                                 .addEvent( three )
                                                                 .addEvent( four )
                                                                 .addEvent( fiveExpected )
                                                                 .addEvent( six )
                                                                 .setMetadata( meta )
                                                                 .build();

        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    void testApplyToEnsembleTimeSeries()
    {
        // Six event times, PT1H apart
        Instant first = Instant.parse( "2079-12-03T00:00:00Z" );
        Instant second = Instant.parse( "2079-12-03T01:00:00Z" );
        Instant third = Instant.parse( "2079-12-03T02:00:00Z" );

        // Six events with two missings
        Event<Ensemble> one = Event.of( first, Ensemble.of( 1.0 ) );
        Event<Ensemble> two = Event.of( second, Ensemble.of( -215.12 ) );
        Event<Ensemble> three = Event.of( third, Ensemble.of( 2.0 ) );

        // Time scale of the event values: instantaneous
        TimeScaleOuter existingScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.MEAN );
        Feature feature = Feature.of( Geometry.getDefaultInstance() );
        TimeSeriesMetadata meta = TimeSeriesMetadata.of( Map.of(),
                                                         existingScale,
                                                         "foo",
                                                         feature,
                                                         "bar" );

        TimeSeries<Ensemble> timeSeries = new Builder<Ensemble>().addEvent( one )
                                                                 .addEvent( two )
                                                                 .addEvent( three )
                                                                 .setMetadata( meta )
                                                                 .build();

        // Missings declaration
        Source source = SourceBuilder.builder()
                                     .missingValue( List.of( -215.12 ) )
                                     .build();
        TimeSeriesTuple tuple = TimeSeriesTuple.ofEnsemble( timeSeries,
                                                            DataSource.builder()
                                                                      .disposition( DataDisposition.CSV_WRES )
                                                                      .source( source )
                                                                      .context( DatasetBuilder.builder()
                                                                                              .sources( List.of( source ) )
                                                                                              .build() )
                                                                      .links( List.of() )
                                                                      .uri( URI.create( "https://fake.uri" ) )
                                                                      .build() );

        MissingValueOperator operator = MissingValueOperator.of();

        TimeSeriesTuple actual = operator.apply( Stream.of( tuple ) )
                                         .findFirst()
                                         .orElseThrow();

        TimeSeries<Ensemble> actualSeries = actual.getEnsembleTimeSeries();

        // Create the expected series
        Event<Ensemble> twoExpected = Event.of( second, Ensemble.of( MissingValues.DOUBLE ) );

        TimeSeries<Ensemble> expectedSeries = new Builder<Ensemble>().addEvent( one )
                                                                     .addEvent( twoExpected )
                                                                     .addEvent( three )
                                                                     .setMetadata( meta )
                                                                     .build();

        assertEquals( expectedSeries, actualSeries );
    }
}
