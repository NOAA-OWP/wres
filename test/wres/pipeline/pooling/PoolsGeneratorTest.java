package wres.pipeline.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.types.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeSeries;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.database.EnsembleRetrieverFactory;
import wres.io.retrieving.database.SingleValuedRetrieverFactory;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link PoolsGenerator}.
 *
 * @author James Brown
 */
class PoolsGeneratorTest
{
    private static final String CFS = "CFS";
    private static final String STREAMFLOW = "STREAMFLOW";

    /** The mocks. */
    private AutoCloseable mocks;

    @BeforeEach
    void setup()
    {
        this.mocks = MockitoAnnotations.openMocks( this );
    }

    /**
     * Tests {@link PoolsGenerator#get()} using project declaration that is representative of system test
     * scenario505 as of commit 43332ccbb45e712722ef2ca52904b18d8f98397c.
     */

    @Test
    void testGetProducesEighteenPoolSuppliersForSingleValuedCase()
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

        Geometry feature = wres.statistics.MessageFactory.getGeometry( "FAKE2" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( feature, feature, null );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );
        Mockito.when( project.getId() )
               .thenReturn( 12345L );
        Mockito.when( project.getVariableName( DatasetOrientation.LEFT ) )
               .thenReturn( "DISCHARGE" );
        Mockito.when( project.getVariableName( DatasetOrientation.RIGHT ) )
               .thenReturn( STREAMFLOW );
        Mockito.when( project.getVariableName( DatasetOrientation.BASELINE ) )
               .thenReturn( null );
        Mockito.when( project.hasBaseline() )
               .thenReturn( false );
        Mockito.when( project.hasProbabilityThresholds() )
               .thenReturn( false );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( featureGroup ) );
        Mockito.when( project.getMeasurementUnit() )
               .thenReturn( CFS );

        Evaluation evaluationDescription = MessageFactory.parse( declaration );

        // Mock a feature-shaped retriever factory
        RetrieverFactory<Double, Double, Double> retrieverFactory = Mockito.mock( SingleValuedRetrieverFactory.class );
        Mockito.when( retrieverFactory.getLeftRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( Stream::of );
        Mockito.when( retrieverFactory.getRightRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( Stream::of );

        PoolFactory poolFactory = PoolFactory.of( project );

        PoolParameters poolParameters = new PoolParameters.Builder().build();
        List<PoolRequest> poolRequests = poolFactory.getPoolRequests( evaluationDescription );

        // Create the actual output
        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>> actual =
                poolFactory.getSingleValuedPools( poolRequests,
                                                  retrieverFactory,
                                                  poolParameters );

        // Assert expected number of suppliers
        Assertions.assertEquals( 18, actual.size() );
    }

    /**
     * Tests {@link PoolsGenerator#get()} using project declaration that is representative of system test
     * scenario505 as of commit 43332ccbb45e712722ef2ca52904b18d8f98397c. While that scenario does not supply ensemble 
     * data, the purpose of this test is to assert that the correct number of pools is generated, rather than the 
     * contents of each pool.
     */

    @Test
    void testGetProducesEighteenPoolSuppliersForEnsembleCase()
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
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .variable( VariableBuilder.builder().name( "STREAMFLOW" )
                                                                .build() )
                                      .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .unit( CFS )
                                            .leadTimes( leadTimes )
                                            .leadTimePools( leadTimePools )
                                            .referenceDates( referenceDates )
                                            .referenceDatePools( referenceTimePools )
                                            .left( left )
                                            .right( right )
                                            .build();

        Geometry feature = wres.statistics.MessageFactory.getGeometry( "FAKE2" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( feature, feature, null );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );
        Mockito.when( project.getId() )
               .thenReturn( 12345L );
        Mockito.when( project.getVariableName( DatasetOrientation.LEFT ) )
               .thenReturn( "DISCHARGE" );
        Mockito.when( project.getVariableName( DatasetOrientation.RIGHT ) )
               .thenReturn( STREAMFLOW );
        Mockito.when( project.getVariableName( DatasetOrientation.BASELINE ) )
               .thenReturn( null );
        Mockito.when( project.hasBaseline() )
               .thenReturn( false );
        Mockito.when( project.hasProbabilityThresholds() )
               .thenReturn( false );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( featureGroup ) );
        Mockito.when( project.getMeasurementUnit() )
               .thenReturn( CFS );

        Evaluation evaluationDescription = MessageFactory.parse( declaration );

        // Mock a feature-shaped retriever factory
        RetrieverFactory<Double, Ensemble, Ensemble> retrieverFactory = Mockito.mock( EnsembleRetrieverFactory.class );
        Mockito.when( retrieverFactory.getLeftRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( Stream::of );
        Mockito.when( retrieverFactory.getRightRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( Stream::of );

        PoolFactory poolFactory = PoolFactory.of( project );

        List<PoolRequest> poolRequests = poolFactory.getPoolRequests( evaluationDescription );

        // Create the actual output
        PoolParameters poolParameters = new PoolParameters.Builder().build();
        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>>> actual =
                poolFactory.getEnsemblePools( poolRequests,
                                              retrieverFactory,
                                              poolParameters );

        // Assert expected number of suppliers
        Assertions.assertEquals( 18, actual.size() );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if( Objects.nonNull( this.mocks ) )
        {
            this.mocks.close();
        }
    }
}
