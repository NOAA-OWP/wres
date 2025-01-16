package wres.pipeline.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeSeries;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.database.SingleValuedRetrieverFactory;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link PoolFactory}.
 *
 * @author James Brown
 */
class PoolFactoryTest
{
    private static final String CFS = "CFS";

    @Test
    void testGetPoolRequestsForEighteenTimeWindowsAndOneFeatureGroupProducesEighteenPoolRequests()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 23 ) )
                                                  .frequency( Duration.ofHours( 17 ) )
                                                  .build();
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
                                     .variable( VariableBuilder.builder().name( "DISCHARGE" )
                                                               .build() )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder().name( "STREAMFLOW" )
                                                                .build() )
                                      .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .unit( CFS )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .left( left )
                                                                        .right( right )
                                                                        .build();

        Evaluation evaluationDescription = MessageFactory.parse( declaration );

        Geometry keyOne = MessageUtilities.getGeometry( "DRRC2", null, null, null );
        Geometry keyTwo = MessageUtilities.getGeometry( "DRRC2HSF", null, null, null );
        Geometry keyThree = MessageUtilities.getGeometry( "DRRC2HSF", null, null, null );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( keyOne, keyTwo, keyThree );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );

        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );
        Mockito.when( project.getMeasurementUnit() )
               .thenReturn( CFS );

        PoolFactory poolFactory = PoolFactory.of( project );
        List<PoolRequest> actual = poolFactory.getPoolRequests( evaluationDescription, null );

        Assertions.assertEquals( 18, actual.size() );
    }

    /**
     * Asserts against the behavior in #101246.
     */

    @Test
    void testGetSingleValuedPoolsProducesFourtyEightPoolSuppliers()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 1 ) )
                                                            .maximum( Duration.ofHours( 24 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 0 ) )
                                                  .frequency( Duration.ofHours( 1 ) )
                                                  .build();
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder().name( "DISCHARGE" )
                                                               .build() )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder().name( "STREAMFLOW" )
                                                                .build() )
                                      .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .unit( CFS )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .left( left )
                                                                        .right( right )
                                                                        .build();

        Evaluation evaluationDescription = MessageFactory.parse( declaration );

        Geometry keyOne = MessageUtilities.getGeometry( "DRRC2HSF", null, null, null );
        Geometry keyTwo = MessageUtilities.getGeometry( "DRRC2HSF", null, null, null );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( keyOne, keyTwo, null );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        GeometryGroup geoGroupTwo = MessageUtilities.getGeometryGroup( "aGroup", geoTuple );

        FeatureGroup groupOne = FeatureGroup.of( geoGroup );
        FeatureGroup groupTwo = FeatureGroup.of( geoGroupTwo );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne, groupTwo ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );
        Mockito.when( project.getMeasurementUnit() )
               .thenReturn( CFS );

        PoolFactory poolFactory = PoolFactory.of( project );

        List<PoolRequest> actual = poolFactory.getPoolRequests( evaluationDescription, null );

        Assertions.assertEquals( 48, actual.size() );

        PoolParameters poolParameters = new PoolParameters.Builder().build();
        RetrieverFactory<Double, Double, Double> retrieverFactory = Mockito.mock( SingleValuedRetrieverFactory.class );
        Mockito.when( retrieverFactory.getLeftRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( () -> Stream.of( TimeSeries.of( null ) ) );
        Mockito.when( retrieverFactory.getRightRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( () -> Stream.of( TimeSeries.of( null ) ) );

        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>> suppliers =
                poolFactory.getSingleValuedPools( actual, retrieverFactory, poolParameters );

        // Assert two feature group names
        Set<FeatureGroup> actualFeatureGroups = suppliers.stream()
                                                         .map( next -> next.getLeft().getMetadata().getFeatureGroup() )
                                                         .collect( Collectors.toSet() );

        Set<FeatureGroup> expectedFeatureGroups = Set.of( groupOne, groupTwo );
        Assertions.assertEquals( expectedFeatureGroups, actualFeatureGroups );

        // Assert the expected number of unique pool identifiers. The actual identifiers will vary since there is one 
        // sequence per class loader and other tests share the class loader
        Set<Long> actualPoolIds = suppliers.stream()
                                           .map( next -> next.getLeft().getMetadata().getPool().getPoolId() )
                                           .collect( Collectors.toSet() );

        Assertions.assertEquals( 48, actualPoolIds.size() );
    }
}
