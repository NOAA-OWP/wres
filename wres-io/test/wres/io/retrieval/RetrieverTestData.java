package wres.io.retrieval;

import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static wres.io.retrieval.RetrieverTestConstants.*;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.system.SystemSettings;

public class RetrieverTestData
{
    static TimeSeries<Double> generateTimeSeriesDoubleOne( ReferenceTimeType referenceTimeType )
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

    static TimeSeries<Double> generateTimeSeriesDoubleTwo( ReferenceTimeType referenceTimeType )
    {
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( referenceTimeType,
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

    static TimeSeries<Double> generateTimeSeriesDoubleThree( ReferenceTimeType referenceTimeType )
    {
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( referenceTimeType,
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

    static TimeSeries<Double> generateTimeSeriesDoubleFour( ReferenceTimeType referenceTimeType )
    {
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( referenceTimeType,
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

    static TimeSeries<Double> generateTimeSeriesDoubleWithNoReferenceTimes()
    {
        // TODO: remove the latest obs reference time type when we allow zero.
        ReferenceTimeType referenceTimeType = ReferenceTimeType.LATEST_OBSERVATION;
        Instant lastObs = Instant.parse( "2023-04-01T10:00:00Z" );

        // Create the expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( referenceTimeType, lastObs ),
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
                .addEvent( Event.of( lastObs, 93.0 ) )
                .build();
    }

    static TimeSeries<Ensemble> generateTimeSeriesEnsembleOne( ReferenceTimeType referenceTimeType )
    {
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( referenceTimeType,
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

    static DataSource generateDataSource( DatasourceType type )
    {
        DataSourceConfig.Source config1 = new DataSourceConfig.Source( FAKE_URI, null, null, null, null );
        DataSourceConfig config2 = new DataSourceConfig( type,
                                                         List.of( config1 ),
                                                         new DataSourceConfig.Variable( VARIABLE_NAME, null ),
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null );
        return DataSource.of( DataSource.DataDisposition.CSV_WRES,
                              config1,
                              config2,
                              Collections.emptyList(),
                              FAKE_URI );
    }

    static DataSource generateBaselineDataSource( DatasourceType type )
    {
        DataSourceConfig.Source config1 = new DataSourceConfig.Source( FAKE_URI, null, null, null, null );
        DataSourceConfig config2 = new DataSourceBaselineConfig( type,
                                                                 List.of( config1 ),
                                                                 new DataSourceConfig.Variable( VARIABLE_NAME, null ),
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 false );
        return DataSource.of( DataSource.DataDisposition.CSV_WRES,
                              config1,
                              config2,
                              Collections.emptyList(),
                              FAKE_URI );
    }
}
