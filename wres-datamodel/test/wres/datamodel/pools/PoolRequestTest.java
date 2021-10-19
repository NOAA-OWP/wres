package wres.datamodel.pools;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

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

        FeatureKey featureOne = FeatureKey.of( "DRRC2" );
        this.featureGroup = FeatureGroup.of( new FeatureTuple( featureOne, featureOne, featureOne ) );

        this.timeWindow = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                              Instant.parse( "1985-12-31T23:59:59Z" ) );

        Pool poolOne = MessageFactory.parse( this.featureGroup,
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
        Pool anotherPool = MessageFactory.parse( this.featureGroup,
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
        Pool yetAnotherPool = MessageFactory.parse( this.featureGroup,
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
            assertTrue( this.poolRequest.equals( anotherPoolRequest ) );
        }

        // Nullity
        assertNotEquals( null, this.poolRequest );
        assertNotEquals( this.poolRequest, null );

        // Unequal cases
        FeatureKey aFeature = FeatureKey.of( "DRRC3" );

        Pool oneMorePool = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( aFeature, aFeature, aFeature ) ),
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
