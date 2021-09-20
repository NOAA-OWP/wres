package wres.datamodel.pools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.pools.Pool.Builder;
import wres.statistics.generated.Evaluation;

/**
 * Tests the {@link Pool}.
 *  
 * @author James Brown
 */

class PoolTest
{

    /**
     * An instance for testing.
     */

    private Pool<String> testPool;

    @BeforeEach
    void runBeforeEachTest()
    {
        Builder<String> builder = new Builder<>();

        this.testPool = builder.addData( List.of( "a", "b", "c" ) )
                               .setMetadata( PoolMetadata.of() )
                               .addDataForBaseline( List.of( "d", "e", "f" ) )
                               .setMetadataForBaseline( PoolMetadata.of() )
                               .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                               .build();
    }

    @Test
    void testGet()
    {
        assertEquals( List.of( "a", "b", "c" ), this.testPool.get() );
    }

    @Test
    void testOf()
    {
        Pool<String> actual = Pool.of( List.of( "d", "e", "f" ), PoolMetadata.of() );

        Pool<String> expected = new Pool.Builder<String>().addData( List.of( "d", "e", "f" ) )
                                                          .setMetadata( PoolMetadata.of() )
                                                          .build();
        assertEquals( expected, actual );

        Pool<String> actualTwo = Pool.of( List.of( "a", "b", "c" ),
                                          PoolMetadata.of(),
                                          List.of( "d", "e", "f" ),
                                          PoolMetadata.of(),
                                          VectorOfDoubles.of( 1, 2, 3 ) );
        assertEquals( this.testPool, actualTwo );
    }

    @Test
    void testHasBaseline()
    {
        assertTrue( this.testPool.hasBaseline() );
    }

    @Test
    void testGetBaselineData()
    {
        Builder<String> builder = new Builder<>();

        Pool<String> expected = builder.addData( "d" )
                                       .addData( "e" )
                                       .addData( "f" )
                                       .setMetadata( PoolMetadata.of() )
                                       .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                       .build();

        assertEquals( expected, this.testPool.getBaselineData() );

        assertNull( expected.getBaselineData() );
    }

    @Test
    void testHasClimatology()
    {
        assertTrue( this.testPool.hasClimatology() );
    }

    @Test
    void testGetClimatology()
    {
        assertEquals( VectorOfDoubles.of( 1, 2, 3 ), this.testPool.getClimatology() );
    }

    @Test
    void testGetMetadata()
    {
        assertEquals( PoolMetadata.of(), this.testPool.getMetadata() );
    }

    @Test
    void testEquals()
    {
        // Reflexive 
        assertTrue( this.testPool.equals( this.testPool ) );

        // Symmetric
        Pool<String> another =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( "d" )
                                     .addDataForBaseline( "e" )
                                     .addDataForBaseline( "f" )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                     .build();

        assertTrue( another.equals( this.testPool ) && this.testPool.equals( another ) );

        // Transitive
        Pool<String> yetAnother =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                     .build();

        assertTrue( this.testPool.equals( another ) && another.equals( yetAnother )
                    && this.testPool.equals( yetAnother ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( this.testPool.equals( another ) );
        }

        // Nullity
        assertNotEquals( null, another );
        assertNotEquals( another, null );

        // Check unequal cases
        Pool<String> unequalOnData =
                new Builder<String>().addData( List.of( "z", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                     .build();

        assertNotEquals( unequalOnData, this.testPool );

        Pool<String> unequalOnMetadata =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of( Evaluation.newBuilder()
                                                                              .setMeasurementUnit( "CFS" )
                                                                              .build(),
                                                                    wres.statistics.generated.Pool.getDefaultInstance() ) )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                     .build();

        assertNotEquals( unequalOnMetadata, this.testPool );

        Pool<String> unequalOnBaseline =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "q" ) )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                     .build();

        assertNotEquals( unequalOnBaseline, this.testPool );

        Pool<String> unequalOnBaselineMeta =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( Evaluation.newBuilder()
                                                                                         .setMeasurementUnit( "CFS" )
                                                                                         .build(),
                                                                               wres.statistics.generated.Pool.getDefaultInstance() ) )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                     .build();

        assertNotEquals( unequalOnBaselineMeta, this.testPool );

        Pool<String> unequalOnClimatology =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 4 ) )
                                     .build();

        assertNotEquals( unequalOnClimatology, this.testPool );

        Pool<String> noBaseline = new Pool.Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                            .setMetadata( PoolMetadata.of() )
                                                            .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                            .build();
        assertNotEquals( this.testPool, noBaseline );

        Pool<String> noClimatology = new Pool.Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                               .setMetadata( PoolMetadata.of() )
                                                               .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                               .setMetadataForBaseline( PoolMetadata.of() )
                                                               .build();

        assertNotEquals( this.testPool, noClimatology );
    }

    @Test
    void testHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.testPool.hashCode(), this.testPool.hashCode() );

        // Consistent when invoked multiple times
        Builder<String> builder = new Builder<>();

        Pool<String> another = builder.addData( List.of( "a", "b", "c" ) )
                                      .setMetadata( PoolMetadata.of() )
                                      .addDataForBaseline( List.of( "d", "e", "f" ) )
                                      .setMetadataForBaseline( PoolMetadata.of() )
                                      .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                      .build();

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testPool.hashCode(), another.hashCode() );
        }
    }

    @Test
    void testAddTwoPoolsToBuilderCreatesMergedPool()
    {
        Pool<String> anotherPool = new Builder<String>().addData( List.of( "d", "e", "f" ) )
                                                        .setMetadata( PoolMetadata.of() )
                                                        .addDataForBaseline( List.of( "a", "b", "c" ) )
                                                        .setMetadataForBaseline( PoolMetadata.of() )
                                                        .setClimatology( VectorOfDoubles.of( 4, 5, 6 ) )
                                                        .build();

        Pool<String> actual = new Builder<String>().addPool( this.testPool )
                                                   .addPool( anotherPool )
                                                   .build();

        Pool<String> expected =
                new Builder<String>().addData( List.of( "a", "b", "c", "d", "e", "f" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f", "a", "b", "c" ) )
                                     .setMetadataForBaseline( PoolMetadata.of() )
                                     .setClimatology( VectorOfDoubles.of( 1, 2, 3, 4, 5, 6 ) )
                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testToString()
    {
        String actual = this.testPool.toString();
        String expected = "Pool[mainMetadata=PoolMetadata[leftDataName=,rightDataName=,baselineDataName=,"
                          + "leftVariableName=,rightVariableName=,baselineVariableName=,isBaselinePool=false,"
                          + "features=<null>,timeWindow=<null>,thresholds=<null>,timeScale=<null>,measurementUnit="
                          + "DIMENSIONLESS],mainData=[a, b, c],baselineMetadata=PoolMetadata[leftDataName=,"
                          + "rightDataName=,baselineDataName=,leftVariableName=,rightVariableName=,"
                          + "baselineVariableName=,isBaselinePool=false,features=<null>,timeWindow=<null>,"
                          + "thresholds=<null>,timeScale=<null>,measurementUnit=DIMENSIONLESS],baselineData=[d, e, f],"
                          + "climatology=[1.0, 2.0, 3.0]]";

        assertEquals( expected, actual );
    }

    @Test
    void testExceptionOnConstructionWithNullEntry()
    {
        Builder<String> builder = new Builder<>();

        builder.addData( Arrays.asList( (String) null ) ).setMetadata( PoolMetadata.of() );

        assertThrows( PoolException.class, () -> builder.build() );
    }

    @Test
    void testExceptionOnConstructionWithNullEntryInBaseline()
    {
        Builder<String> builder = new Builder<String>().addData( "Foo" )
                                                       .setMetadata( PoolMetadata.of() )
                                                       .addDataForBaseline( Arrays.asList( (String) null ) )
                                                       .setMetadataForBaseline( PoolMetadata.of() );

        assertThrows( PoolException.class, () -> builder.build() );
    }

    @Test
    void testExceptionOnConstructionWithNaNInClimatology()
    {
        VectorOfDoubles climatology = VectorOfDoubles.of( Double.NaN );

        Builder<String> builder = new Builder<String>().addData( List.of( "OK" ) )
                                                       .setClimatology( climatology )
                                                       .setMetadata( PoolMetadata.of() );

        assertThrows( PoolException.class, () -> builder.build() );
    }

    @Test
    void testExceptionOnConstructionWithEmptyClimatology()
    {
        Builder<String> builder = new Builder<String>().addData( List.of( "OK" ) )
                                                       .setClimatology( VectorOfDoubles.of() )
                                                       .setMetadata( PoolMetadata.of() );

        assertThrows( PoolException.class, () -> builder.build() );
    }

    @Test
    void testExceptionOnConstructionWithoutMetadata()
    {
        assertThrows( PoolException.class, () -> new Builder<String>().addData( List.of( "OK" ) ).build() );
    }

    @Test
    void testExceptionOnConstructionWithoutBaselineMetadata()
    {
        Builder<String> builder = new Builder<String>().addData( "Foo" )
                                                       .setMetadata( PoolMetadata.of() )
                                                       .addDataForBaseline( "Bar" );

        assertThrows( PoolException.class, () -> builder.build() );
    }
}
