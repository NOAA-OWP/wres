package wres.datamodel.pools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link PoolRequest}.
 *
 * @author James Brown
 */

class PoolRequestTest
{
    private PoolRequest poolRequest;
    private PoolMetadata poolMetadata;
    private Evaluation evaluation;
    private TimeWindowOuter timeWindow;
    private FeatureGroup featureGroup;

    @BeforeEach
    void setUpBeforeEachTest()
    {
        this.evaluation = Evaluation.newBuilder()
                                    .setRightVariableName( "SQIN" )
                                    .setRightDataName( "HEFS" )
                                    .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                    .build();

        Geometry geometry = wres.statistics.MessageFactory.getGeometry( "DRRC2" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        this.featureGroup = FeatureGroup.of( geoGroup );

        this.timeWindow = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                            Instant.parse( "1985-12-31T23:59:59Z" ) ) );

        Pool poolOne = MessageFactory.getPool( this.featureGroup,
                                               this.timeWindow,
                                               null,
                                               null,
                                               false,
                                               1 );

        this.poolMetadata = PoolMetadata.of( this.evaluation, poolOne );

        this.poolRequest = PoolRequest.of( this.poolMetadata );
    }

    @Test
    void testEquals()
    {
        // Reflexive 
        assertEquals( this.poolRequest, this.poolRequest );

        // Symmetric
        Pool anotherPool = MessageFactory.getPool( this.featureGroup,
                                                   this.timeWindow,
                                                   null,
                                                   null,
                                                   false,
                                                   1 );

        PoolMetadata anotherPoolMetadata = PoolMetadata.of( this.evaluation, anotherPool );
        PoolRequest anotherPoolRequest = PoolRequest.of( anotherPoolMetadata );

        assertTrue( anotherPoolRequest.equals( this.poolRequest )
                    && this.poolRequest.equals( anotherPoolRequest ) );

        // Transitive
        Pool yetAnotherPool = MessageFactory.getPool( this.featureGroup,
                                                      this.timeWindow,
                                                      null,
                                                      null,
                                                      false,
                                                      1 );

        PoolMetadata yetAnotherPoolMetadata = PoolMetadata.of( this.evaluation, yetAnotherPool );
        PoolRequest yetAnotherPoolRequest = PoolRequest.of( yetAnotherPoolMetadata );

        assertTrue( this.poolRequest.equals( anotherPoolRequest )
                    && anotherPoolRequest.equals( yetAnotherPoolRequest )
                    && this.poolRequest.equals( yetAnotherPoolRequest ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.poolRequest, anotherPoolRequest );
        }

        // Nullity
        assertNotEquals( null, this.poolRequest );
        assertNotEquals( this.poolRequest, null );

        // Unequal cases
        Geometry geometry = wres.statistics.MessageFactory.getGeometry( "DRRC3" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup group = FeatureGroup.of( geoGroup );

        Pool oneMorePool = MessageFactory.getPool( group,
                                                   this.timeWindow,
                                                   null,
                                                   null,
                                                   false,
                                                   1 );

        PoolMetadata oneMorePoolMetadata = PoolMetadata.of( this.evaluation, oneMorePool );
        PoolRequest oneMorePoolRequest = PoolRequest.of( oneMorePoolMetadata );

        assertNotEquals( this.poolRequest, oneMorePoolRequest );

        assertNotEquals( this.poolRequest, PoolRequest.of( anotherPoolMetadata, anotherPoolMetadata ) );
    }

    @Test
    void testCompareTo()
    {
        assertEquals( 0, this.poolRequest.compareTo( this.poolRequest ) );

        PoolRequest withBaseline = PoolRequest.of( this.poolMetadata, this.poolMetadata );

        assertTrue( this.poolRequest.compareTo( withBaseline ) < 0 );

        assertTrue( withBaseline.compareTo( this.poolRequest ) > 0 );

        assertEquals( 0, withBaseline.compareTo( withBaseline ) );

        PoolMetadata newMetadata = PoolMetadata.of( this.poolMetadata, TimeScaleOuter.of( Duration.ofHours( 1 ) ) );
        PoolRequest newPoolRequest = PoolRequest.of( newMetadata );

        assertTrue( newPoolRequest.compareTo( this.poolRequest ) > 0 );
    }

    @Test
    void testHashCode()
    {
        // Equals consistent with hashcode
        assertEquals( this.poolRequest.hashCode(), this.poolRequest.hashCode() );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.poolRequest.hashCode(), this.poolRequest.hashCode() );
        }
    }

    @Test
    void testHasBaseline()
    {
        assertFalse( this.poolRequest.hasBaseline() );

        assertTrue( PoolRequest.of( this.poolMetadata, this.poolMetadata ).hasBaseline() );
    }

    @Test
    void testToString()
    {
        assertTrue( this.poolRequest.toString().contains( "PoolRequest" ) );
    }

}
