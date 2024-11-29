package wres.pipeline.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.CovariateDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link CovariateFilter}.
 *
 * @author James Brown
 */

class CovariateFilterTest
{
    @Test
    void testUnconditionalFilter()
    {
        Dataset covariateData = DatasetBuilder.builder()
                                              .type( DataType.OBSERVATIONS )
                                              .variable( VariableBuilder.builder()
                                                                        .name( "foo" )
                                                                        .build() )
                                              .build();
        CovariateDataset covariateDataset = CovariateDatasetBuilder.builder()
                                                                   .dataset( covariateData )
                                                                   .featureNameOrientation( DatasetOrientation.LEFT )
                                                                   .build();

        // Unconditional filter
        Predicate<Double> filter = d -> true;

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of();

        Covariate<Double> covariate = new Covariate<>( covariateDataset, filter, desiredTimeScale, null );

        Feature feature = Feature.of( MessageFactory.getGeometry( "feature" ) );
        TimeSeriesMetadata covariateMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                      TimeScaleOuter.of(),
                                                                      "bar",
                                                                      feature,
                                                                      "covariate_unit" );

        TimeSeries<Double> covariateSeries = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), 0.4 ) )
                .setMetadata( covariateMetadata )
                .build();

        Supplier<Stream<TimeSeries<Double>>> covariateSupplier = () -> Stream.of( covariateSeries );

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                TimeSeriesMetadata.of( Map.of(),
                                       desiredTimeScale,
                                       "foo",
                                       feature,
                                       "baz" );

        Event<Pair<Double, Double>> eventOne =
                Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), Pair.of( 123.0, 345.0 ) );
        Event<Pair<Double, Double>> eventTwo =
                Event.of( Instant.parse( "2123-12-01T08:00:00Z" ), Pair.of( 456.0, 567.0 ) );
        TimeSeries<Pair<Double, Double>> poolData =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( eventOne )
                                                              .addEvent( eventTwo )
                                                              .build();

        // Basic metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "foo" )
                                          .setMeasurementUnit( "bar" )
                                          .build();

        GeometryTuple geoTuple = wres.datamodel.messages.MessageFactory.getGeometryTuple( feature, feature, null );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryGroup geoGroup =
                wres.statistics.MessageFactory.getGeometryGroup( featureTuple.toStringShort(), geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        wres.statistics.generated.Pool poolDescription =
                wres.datamodel.messages.MessageFactory.getPool( featureGroup,
                                                                null,
                                                                null,
                                                                null,
                                                                false );

        PoolMetadata metadata = PoolMetadata.of( evaluation, poolDescription );
        Pool<TimeSeries<Pair<Double, Double>>> pool =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolData )
                                                                    .setMetadata( metadata )
                                                                    .build();

        CovariateFilter<Double, Double> covariateFilter = CovariateFilter.of( pool, covariate, covariateSupplier );

        Pool<TimeSeries<Pair<Double, Double>>> actual = covariateFilter.get();

        Event<Pair<Double, Double>> eventOneExpected =
                Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), Pair.of( 123.0, 345.0 ) );
        TimeSeries<Pair<Double, Double>> poolDataExpected =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( eventOneExpected )
                                                              .build();

        Pool<TimeSeries<Pair<Double, Double>>> expected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolDataExpected )
                                                                    .setMetadata( metadata )
                                                                    .build();

        assertEquals( expected, actual );
    }

    @Test
    void testFilterWithTimeShift()
    {
        Dataset covariateData = DatasetBuilder.builder()
                                              .type( DataType.OBSERVATIONS )
                                              .variable( VariableBuilder.builder()
                                                                        .name( "foo" )
                                                                        .build() )
                                              // Add 4 hours
                                              .timeShift( Duration.ofHours( 4 ) )
                                              .build();
        CovariateDataset covariateDataset = CovariateDatasetBuilder.builder()
                                                                   .dataset( covariateData )
                                                                   .featureNameOrientation( DatasetOrientation.LEFT )
                                                                   .build();

        Predicate<Double> filter = d -> d > 0.5;

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of();

        Covariate<Double> covariate = new Covariate<>( covariateDataset, filter, desiredTimeScale, null );

        Feature feature = Feature.of( MessageFactory.getGeometry( "feature" ) );
        TimeSeriesMetadata covariateMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                      TimeScaleOuter.of(),
                                                                      "bar",
                                                                      feature,
                                                                      "covariate_unit" );

        TimeSeries<Double> covariateSeries = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T03:00:00Z" ), 0.4 ) )
                .addEvent( Event.of( Instant.parse( "2123-12-01T04:00:00Z" ), 4.0 ) )
                .setMetadata( covariateMetadata )
                .build();

        Supplier<Stream<TimeSeries<Double>>> covariateSupplier = () -> Stream.of( covariateSeries );

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                TimeSeriesMetadata.of( Map.of(),
                                       desiredTimeScale,
                                       "foo",
                                       feature,
                                       "baz" );

        Event<Pair<Double, Double>> eventOne =
                Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), Pair.of( 123.0, 345.0 ) );
        Event<Pair<Double, Double>> eventTwo =
                Event.of( Instant.parse( "2123-12-01T08:00:00Z" ), Pair.of( 456.0, 567.0 ) );
        TimeSeries<Pair<Double, Double>> poolData =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( eventOne )
                                                              .addEvent( eventTwo )
                                                              .build();

        // Basic metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "foo" )
                                          .setMeasurementUnit( "bar" )
                                          .build();

        GeometryTuple geoTuple = wres.datamodel.messages.MessageFactory.getGeometryTuple( feature, feature, null );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryGroup geoGroup =
                wres.statistics.MessageFactory.getGeometryGroup( featureTuple.toStringShort(), geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        wres.statistics.generated.Pool poolDescription =
                wres.datamodel.messages.MessageFactory.getPool( featureGroup,
                                                                null,
                                                                null,
                                                                null,
                                                                false );

        PoolMetadata metadata = PoolMetadata.of( evaluation, poolDescription );
        Pool<TimeSeries<Pair<Double, Double>>> pool =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolData )
                                                                    .setMetadata( metadata )
                                                                    .build();

        CovariateFilter<Double, Double> covariateFilter = CovariateFilter.of( pool, covariate, covariateSupplier );

        Pool<TimeSeries<Pair<Double, Double>>> actual = covariateFilter.get();

        Event<Pair<Double, Double>> eventTwoExpected =
                Event.of( Instant.parse( "2123-12-01T08:00:00Z" ), Pair.of( 456.0, 567.0 ) );
        TimeSeries<Pair<Double, Double>> poolDataExpected =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( eventTwoExpected )
                                                              .build();

        Pool<TimeSeries<Pair<Double, Double>>> expected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolDataExpected )
                                                                    .setMetadata( metadata )
                                                                    .build();

        assertEquals( expected, actual );
    }

    @Test
    void testFilterWithUpscaling()
    {
        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                      TimeScale.TimeScaleFunction.TOTAL );
        wres.config.yaml.components.TimeScale existingTimeScale =
                new wres.config.yaml.components.TimeScale( timeScale.getTimeScale() );

        Dataset covariateData = DatasetBuilder.builder()
                                              .type( DataType.OBSERVATIONS )
                                              .variable( VariableBuilder.builder()
                                                                        .name( "foo" )
                                                                        .build() )
                                              .timeScale( existingTimeScale )
                                              .build();
        CovariateDataset covariateDataset = CovariateDatasetBuilder.builder()
                                                                   .dataset( covariateData )
                                                                   .featureNameOrientation( DatasetOrientation.LEFT )
                                                                   .build();
        Predicate<Double> filter = d -> d > 0.5;

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 2 ),
                                                             TimeScale.TimeScaleFunction.TOTAL );

        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        Covariate<Double> covariate = new Covariate<>( covariateDataset, filter, desiredTimeScale, upscaler );

        Feature feature = Feature.of( MessageFactory.getGeometry( "feature" ) );
        TimeSeriesMetadata covariateMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                      timeScale,
                                                                      "bar",
                                                                      feature,
                                                                      "covariate_unit" );

        TimeSeries<Double> covariateSeries = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), 0.2 ) )
                .addEvent( Event.of( Instant.parse( "2123-12-01T08:00:00Z" ), 0.3 ) )
                .addEvent( Event.of( Instant.parse( "2123-12-01T09:00:00Z" ), 0.2 ) )
                .addEvent( Event.of( Instant.parse( "2123-12-01T10:00:00Z" ), 0.4 ) )
                .setMetadata( covariateMetadata )
                .build();

        Supplier<Stream<TimeSeries<Double>>> covariateSupplier = () -> Stream.of( covariateSeries );

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                TimeSeriesMetadata.of( Map.of(),
                                       desiredTimeScale,
                                       "foo",
                                       feature,
                                       "baz" );

        Event<Pair<Double, Double>> eventOne =
                Event.of( Instant.parse( "2123-12-01T08:00:00Z" ), Pair.of( 123.0, 345.0 ) );
        Event<Pair<Double, Double>> eventTwo =
                Event.of( Instant.parse( "2123-12-01T10:00:00Z" ), Pair.of( 456.0, 567.0 ) );
        TimeSeries<Pair<Double, Double>> poolData =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( eventOne )
                                                              .addEvent( eventTwo )
                                                              .build();

        // Basic metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "foo" )
                                          .setMeasurementUnit( "bar" )
                                          .build();

        GeometryTuple geoTuple = wres.datamodel.messages.MessageFactory.getGeometryTuple( feature, feature, null );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryGroup geoGroup =
                wres.statistics.MessageFactory.getGeometryGroup( featureTuple.toStringShort(), geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        wres.statistics.generated.Pool poolDescription =
                wres.datamodel.messages.MessageFactory.getPool( featureGroup,
                                                                null,
                                                                desiredTimeScale,
                                                                null,
                                                                false );

        PoolMetadata metadata = PoolMetadata.of( evaluation, poolDescription );
        Pool<TimeSeries<Pair<Double, Double>>> pool =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolData )
                                                                    .setMetadata( metadata )
                                                                    .build();

        CovariateFilter<Double, Double> covariateFilter = CovariateFilter.of( pool, covariate, covariateSupplier );

        Pool<TimeSeries<Pair<Double, Double>>> actual = covariateFilter.get();

        Event<Pair<Double, Double>> eventTwoExpected =
                Event.of( Instant.parse( "2123-12-01T10:00:00Z" ), Pair.of( 456.0, 567.0 ) );
        TimeSeries<Pair<Double, Double>> poolDataExpected =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( eventTwoExpected )
                                                              .build();

        Pool<TimeSeries<Pair<Double, Double>>> expected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolDataExpected )
                                                                    .setMetadata( metadata )
                                                                    .build();

        assertEquals( expected, actual );
    }
}