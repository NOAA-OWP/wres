package wres.io;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.space.Feature;
import wres.datamodel.types.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;

import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

public class TestData
{
    static final Feature FEATURE = Feature.of( MessageFactory.getGeometry( "F" ) );
    static final String VARIABLE_NAME = "Q";
    static final String UNIT = "[ft_i]3/s";
    static final URI FAKE_URI = URI.create( "file:///some.csv" );
    static final Instant T2023_04_01T00_00_00Z = Instant.parse( "2023-04-01T00:00:00Z" );
    static final Instant T2023_04_01T01_00_00Z = Instant.parse( "2023-04-01T01:00:00Z" );
    static final Instant T2023_04_01T03_00_00Z = Instant.parse( "2023-04-01T03:00:00Z" );
    static final Instant T2023_04_01T04_00_00Z = Instant.parse( "2023-04-01T04:00:00Z" );
    static final Instant T2023_04_01T06_00_00Z = Instant.parse( "2023-04-01T06:00:00Z" );
    static final Instant T2023_04_01T07_00_00Z = Instant.parse( "2023-04-01T07:00:00Z" );
    static final Instant T2023_04_01T17_00_00Z = Instant.parse( "2023-04-01T17:00:00Z" );

    public static TimeSeries<Double> generateTimeSeriesDoubleOne( ReferenceTimeType referenceTimeType )
    {
        // Create the first expected series
        TimeSeriesMetadata metadataOne =
                TimeSeriesMetadata.of( Map.of( referenceTimeType,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        return new TimeSeries.Builder<Double>().setMetadata( metadataOne )
                                               .addEvent( Event.of( T2023_04_01T01_00_00Z, 30.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                                               .build();
    }

    public static TimeSeries<Double> generateTimeSeriesDoubleTwo()
    {
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        return new TimeSeries.Builder<Double>().setMetadata( metadataTwo )
                                               .addEvent( Event.of( T2023_04_01T04_00_00Z, 72.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 79.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 86.0 ) )
                                               .build();
    }

    public static TimeSeries<Double> generateTimeSeriesDoubleThree()
    {
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        return new TimeSeries.Builder<Double>().setMetadata( metadataThree )
                                               .addEvent( Event.of( T2023_04_01T07_00_00Z, 114.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 121.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T09:00:00Z" ), 128.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 135.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T11:00:00Z" ), 142.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T12:00:00Z" ), 149.0 ) )
                                               .build();
    }

    public static TimeSeries<Double> generateTimeSeriesDoubleFour()
    {
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               T2023_04_01T17_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        return new TimeSeries.Builder<Double>().setMetadata( metadataThree )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T18:00:00Z" ), 65.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T19:00:00Z" ), 72.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T20:00:00Z" ), 79.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T21:00:00Z" ), 86.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T22:00:00Z" ), 93.0 ) )
                                               .build();
    }

    public static TimeSeries<Double> generateTimeSeriesDoubleWithNoReferenceTimes()
    {
        // Create the expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of(),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        return new TimeSeries.Builder<Double>()
                .setMetadata( metadata )
                .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T09:00:00Z" ), 86.0 ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 93.0 ) )
                .build();
    }

    public static TimeSeries<Ensemble> generateTimeSeriesEnsembleOne()
    {
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        Ensemble.Labels labels = Ensemble.Labels.of( "123", "567", "456" );
        Ensemble one = Ensemble.of( new double[] { 30.0, 65.0, 100.0 }, labels );
        Ensemble two = Ensemble.of( new double[] { 37.0, 72.0, 107.0 }, labels );
        Ensemble three = Ensemble.of( new double[] { 44.0, 79.0, 114.0 }, labels );
        Ensemble four = Ensemble.of( new double[] { 51.0, 86.0, 121.0 }, labels );
        Ensemble five = Ensemble.of( new double[] { 58.0, 93.0, 128.0 }, labels );
        return new TimeSeries.Builder<Ensemble>()
                .setMetadata( metadataThree )
                .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), one ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), two ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), three ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), four ) )
                .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), five ) )
                .build();
    }

    public static DataSource generateDataSource( DatasetOrientation orientation,
                                          DataType type )
    {
        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( FAKE_URI )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .type( type )
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( VARIABLE_NAME )
                                                                  .build() )
                                        .build();

        return DataSource.of( DataSource.DataDisposition.CSV_WRES,
                              fakeDeclarationSource,
                              dataset,
                              Collections.emptyList(),
                              FAKE_URI,
                              orientation );
    }

    public static DataSource generateBaselineDataSource( DataType type )
    {
        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( FAKE_URI )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .type( type )
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( VARIABLE_NAME )
                                                                  .build() )
                                        .build();

        return DataSource.of( DataSource.DataDisposition.CSV_WRES,
                              fakeDeclarationSource,
                              dataset,
                              Collections.emptyList(),
                              FAKE_URI,
                              DatasetOrientation.BASELINE );
    }
}
