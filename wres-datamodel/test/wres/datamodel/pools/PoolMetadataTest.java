package wres.datamodel.pools;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import wres.config.MetricConstants;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.config.yaml.components.ThresholdOperator;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link PoolMetadata}.
 * 
 * @author James Brown
 */

class PoolMetadataTest
{
    private static final String HEFS = "HEFS";
    private static final String SQIN = "SQIN";
    private static final String TEST_DIMENSION = "SOME_DIM";
    private static final String DRRC3 = "DRRC3";
    private static final String DRRC2 = "DRRC2";
    private static final String THIRD_TIME = "2000-02-02T00:00:00Z";
    private static final String SECOND_TIME = "1986-01-01T00:00:00Z";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /**
     * Tests construction of the {@link PoolMetadata} using the various construction options.
     */

    @Test
    void testBuildProducesNonNullInstances()
    {
        assertNotNull( PoolMetadata.of() );

        assertNotNull( PoolMetadata.of( Evaluation.newBuilder()
                                                  .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                  .build(),
                                        Pool.getDefaultInstance() ) );

        Geometry geo = wres.statistics.MessageFactory.getGeometry( "A" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geo, geo, geo );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup group = FeatureGroup.of( geoGroup );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "B" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool poolOne = MessageFactory.getPool( group,
                                               null,
                                               null,
                                               null,
                                               false,
                                               1 );

        assertNotNull( PoolMetadata.of( evaluation, poolOne ) );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                  ThresholdOperator.EQUAL,
                                                                                  ThresholdOrientation.LEFT ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), thresholds ) );

        TimeWindowOuter timeWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( THIRD_TIME ),
                                                                                                       Instant.parse( THIRD_TIME ) ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), timeWindow ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), timeWindow, thresholds ) );

        Pool pool = MessageFactory.getPool( group,
                                            timeWindow,
                                            TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                               TimeScaleFunction.MEAN ),
                                            thresholds,
                                            false,
                                            1 );

        assertNotNull( PoolMetadata.of( evaluation, pool ) );
    }

    /**
     * Test {@link PoolMetadata#equals(Object)}.
     */

    @Test
    void testEquals()
    {
        assertEquals( PoolMetadata.of(), PoolMetadata.of() );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Geometry geoOne = wres.statistics.MessageFactory.getGeometry( DRRC2 );
        GeometryTuple geoTupleOne = wres.statistics.MessageFactory.getGeometryTuple( geoOne, geoOne, geoOne );
        GeometryGroup geoGroupOne = wres.statistics.MessageFactory.getGeometryGroup( null, geoTupleOne );
        FeatureGroup featureGroupOne = FeatureGroup.of( geoGroupOne );

        Pool pool = MessageFactory.getPool( featureGroupOne,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        // Reflexive
        assertEquals( m1, m1 );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo = MessageFactory.getPool( featureGroupOne, null, null, null, false, 1 );

        PoolMetadata m2 = PoolMetadata.of( evaluationTwo, poolTwo );

        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( TEST_DIMENSION )
                                               .build();

        Pool poolThree = MessageFactory.getPool( featureGroupOne, null, null, null, false, 1 );

        PoolMetadata m3 = PoolMetadata.of( evaluationThree, poolThree );

        Evaluation evaluationFour = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFour = MessageFactory.getPool( featureGroupOne, null, null, null, false, 1 );

        PoolMetadata m4 = PoolMetadata.of( evaluationFour, poolFour );
        assertEquals( m3, m4 );
        assertNotEquals( m1, m3 );

        // Transitive
        Evaluation evaluationFive = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFive = MessageFactory.getPool( featureGroupOne,
                                                null,
                                                null,
                                                null,
                                                false,
                                                1 );

        PoolMetadata m4t = PoolMetadata.of( evaluationFive, poolFive );
        assertEquals( m4, m4t );
        assertEquals( m3, m4t );

        // Unequal
        Geometry geoTwo = wres.statistics.MessageFactory.getGeometry( DRRC3 );
        GeometryTuple geoTupleTwo = wres.statistics.MessageFactory.getGeometryTuple( geoTwo, geoTwo, geoTwo );
        GeometryGroup geoGroupTwo = wres.statistics.MessageFactory.getGeometryGroup( null, geoTupleTwo );
        FeatureGroup featureGroupTwo = FeatureGroup.of( geoGroupTwo );

        Pool poolSix = MessageFactory.getPool( featureGroupTwo, null, null, null, false, 1 );
        PoolMetadata m5 = PoolMetadata.of( evaluationFive, poolSix );
        assertNotEquals( m4, m5 );
        PoolMetadata m5NoDim = PoolMetadata.of( Evaluation.newBuilder()
                                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                          .build(),
                                                poolSix );
        assertNotEquals( m5, m5NoDim );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( m1, m2 );
        }
        // Add a time window
        TimeWindowOuter firstWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                                                        Instant.parse( SECOND_TIME ) ) );

        Pool poolSeven = MessageFactory.getPool( featureGroupTwo,
                                                 firstWindow,
                                                 null,
                                                 null,
                                                 false,
                                                 1 );

        PoolMetadata m6 = PoolMetadata.of( evaluationFive, poolSeven );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                                                         Instant.parse( SECOND_TIME ) ) );

        Pool poolEight = MessageFactory.getPool( featureGroupTwo,
                                                 secondWindow,
                                                 null,
                                                 null,
                                                 false,
                                                 1 );

        PoolMetadata m7 = PoolMetadata.of( evaluationFive, poolEight );

        assertEquals( m6, m7 );
        assertEquals( m7, m6 );
        assertNotEquals( m3, m6 );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                                                        Instant.parse( SECOND_TIME ),
                                                                                                        Instant.parse( FIRST_TIME ),
                                                                                                        Instant.parse( SECOND_TIME ) ) );

        Pool poolNine = MessageFactory.getPool( featureGroupTwo,
                                                thirdWindow,
                                                null,
                                                null,
                                                false,
                                                1 );

        PoolMetadata m8 = PoolMetadata.of( evaluationFive, poolNine );

        assertNotEquals( m6, m8 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        Pool poolTen = MessageFactory.getPool( featureGroupTwo,
                                               thirdWindow,
                                               null,
                                               thresholds,
                                               false,
                                               1 );

        PoolMetadata m9 = PoolMetadata.of( evaluationFive, poolTen );

        assertNotEquals( m8, m9 );

        PoolMetadata m10 = PoolMetadata.of( evaluationFive, poolTen );

        assertEquals( m9, m10 );

        // Add a project declaration
        Dataset observedDataset = DatasetBuilder.builder()
                .type( DataType.OBSERVATIONS )
                                                .build();

        Dataset predictedDataset = DatasetBuilder.builder()
                .type( DataType.SINGLE_VALUED_FORECASTS )
                                                 .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.BIAS_FRACTION, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                       .left( observedDataset )
                                                                       .right( predictedDataset )
                                                                       .unit( "CMS" )
                                                                       .metrics( metrics )
                                                                       .build();

        Evaluation evaluationSix = MessageFactory.parse( declaration );

        Pool poolEleven = MessageFactory.getPool( featureGroupTwo,
                                                  thirdWindow,
                                                  null,
                                                  thresholds,
                                                  false,
                                                  1 );

        PoolMetadata m11 = PoolMetadata.of( evaluationSix, poolEleven );

        Evaluation evaluationSeven = MessageFactory.parse( declaration );

        Pool poolTwelve = MessageFactory.getPool( featureGroupTwo,
                                                  thirdWindow,
                                                  null,
                                                  thresholds,
                                                  false,
                                                  1 );

        PoolMetadata m12 = PoolMetadata.of( evaluationSeven, poolTwelve );

        assertEquals( m11, m12 );

        // Add a time scale
        Pool poolThirteen = MessageFactory.getPool( featureGroupTwo,
                                                    thirdWindow,
                                                    TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ),
                                                    thresholds,
                                                    false,
                                                    1 );


        PoolMetadata m13 = PoolMetadata.of( evaluationSeven, poolThirteen );

        PoolMetadata m14 = PoolMetadata.of( evaluationSeven, poolThirteen );

        Pool poolFourteen = MessageFactory.getPool( featureGroupTwo,
                                                    thirdWindow,
                                                    TimeScaleOuter.of( Duration.ofDays( 2 ), TimeScaleFunction.MEAN ),
                                                    thresholds,
                                                    false,
                                                    1 );


        PoolMetadata m15 = PoolMetadata.of( evaluationSeven, poolFourteen );

        assertEquals( m13, m14 );

        assertNotEquals( m13, m15 );

        // Null check
        assertNotEquals( null, m6 );

        // Other type check
        assertNotEquals( 2.0, m6 );
    }

    /**
     * Test {@link PoolMetadata#hashCode()}.
     */

    @Test
    void testHashcode()
    {
        // Equal
        assertEquals( PoolMetadata.of().hashCode(), PoolMetadata.of().hashCode() );

        Geometry geoOne = wres.statistics.MessageFactory.getGeometry( DRRC2 );
        GeometryTuple geoTupleOne = wres.statistics.MessageFactory.getGeometryTuple( geoOne, geoOne, geoOne );
        GeometryGroup geoGroupOne = wres.statistics.MessageFactory.getGeometryGroup( null, geoTupleOne );
        FeatureGroup featureGroupOne = FeatureGroup.of( geoGroupOne );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.getPool( featureGroupOne,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );
        assertEquals( m1.hashCode(), m1.hashCode() );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo = MessageFactory.getPool( featureGroupOne, null, null, null, false, 1 );

        PoolMetadata m2 = PoolMetadata.of( evaluationTwo, poolTwo );
        assertEquals( m1.hashCode(), m2.hashCode() );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( PoolMetadataTest.TEST_DIMENSION )
                                               .build();

        Pool poolThree = MessageFactory.getPool( featureGroupOne, null, null, null, false, 1 );

        PoolMetadata m3 = PoolMetadata.of( evaluationThree, poolThree );

        Pool poolFour = MessageFactory.getPool( featureGroupOne, null, null, null, false, 1 );

        PoolMetadata m4 = PoolMetadata.of( evaluationThree, poolFour );
        assertEquals( m3.hashCode(), m4.hashCode() );

        Pool poolFive = MessageFactory.getPool( featureGroupOne,
                                                null,
                                                null,
                                                null,
                                                false,
                                                1 );

        PoolMetadata m4t = PoolMetadata.of( evaluationThree, poolFive );
        assertEquals( m4.hashCode(), m4t.hashCode() );
        assertEquals( m3.hashCode(), m4t.hashCode() );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( m1.hashCode(), m2.hashCode() );
        }

        // Add a time window
        TimeWindowOuter firstWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                                                        Instant.parse( SECOND_TIME ) ) );

        Geometry geoTwo = wres.statistics.MessageFactory.getGeometry( DRRC3 );
        GeometryTuple geoTupleTwo = wres.statistics.MessageFactory.getGeometryTuple( geoTwo, geoTwo, geoTwo );
        GeometryGroup geoGroupTwo = wres.statistics.MessageFactory.getGeometryGroup( null, geoTupleTwo );
        FeatureGroup featureGroupTwo = FeatureGroup.of( geoGroupTwo );

        Pool poolSix = MessageFactory.getPool( featureGroupTwo,
                                               firstWindow,
                                               null,
                                               null,
                                               false,
                                               1 );

        PoolMetadata m6 = PoolMetadata.of( evaluationThree, poolSix );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                                                         Instant.parse( SECOND_TIME ) ) );

        Pool poolSeven = MessageFactory.getPool( featureGroupTwo,
                                                 secondWindow,
                                                 null,
                                                 null,
                                                 false,
                                                 1 );

        PoolMetadata m7 = PoolMetadata.of( evaluationThree, poolSeven );

        assertEquals( m6.hashCode(), m7.hashCode() );
        assertEquals( m7.hashCode(), m6.hashCode() );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                                                        Instant.parse( SECOND_TIME ),
                                                                                                        Instant.parse( FIRST_TIME ),
                                                                                                        Instant.parse( SECOND_TIME ) ) );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        Pool poolEight = MessageFactory.getPool( featureGroupTwo,
                                                 thirdWindow,
                                                 null,
                                                 thresholds,
                                                 false,
                                                 1 );

        PoolMetadata m9 = PoolMetadata.of( evaluationThree, poolEight );

        PoolMetadata m10 = PoolMetadata.of( evaluationThree, poolEight );

        assertEquals( m9.hashCode(), m10.hashCode() );

        // Add a project declaration
        Dataset observedDataset = DatasetBuilder.builder()
                                                .type( DataType.OBSERVATIONS )
                                                .build();

        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .type( DataType.SINGLE_VALUED_FORECASTS )
                                                 .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.BIAS_FRACTION, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( observedDataset )
                                                                        .right( predictedDataset )
                                                                        .unit( "CMS" )
                                                                        .metrics( metrics )
                                                                        .build();

        Evaluation evaluationFour = MessageFactory.parse( declaration );

        Pool poolNine = MessageFactory.getPool( featureGroupTwo,
                                                thirdWindow,
                                                null,
                                                thresholds,
                                                false,
                                                1 );

        PoolMetadata m11 = PoolMetadata.of( evaluationFour, poolNine );

        Evaluation evaluationFive = MessageFactory.parse( declaration );

        Pool poolTen = MessageFactory.getPool( featureGroupTwo,
                                               thirdWindow,
                                               null,
                                               thresholds,
                                               false,
                                               1 );

        PoolMetadata m12 = PoolMetadata.of( evaluationFive, poolTen );

        assertEquals( m11.hashCode(), m12.hashCode() );
    }

    @Test
    void testGetMetadata()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( SQIN )
                                          .build();

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow() );

        Geometry geoOne = wres.statistics.MessageFactory.getGeometry( DRRC2 );
        Geometry geoTwo = wres.statistics.MessageFactory.getGeometry( DRRC3 );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geoOne, geoTwo, null );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( featureGroup,
                                            timeWindow,
                                            TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                               TimeScaleFunction.MEAN ),
                                            thresholds,
                                            false,
                                            1 );

        PoolMetadata metadata = PoolMetadata.of( evaluation, pool );

        assertEquals( featureGroup, metadata.getFeatureGroup() );
    }

}
