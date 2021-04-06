package wres.datamodel.pools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.pools.BasicPool.Builder;
import wres.statistics.generated.Evaluation;

/**
 * Tests the {@link BasicPool}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BasicPoolTest
{

    /**
     * An instance for testing.
     */

    private BasicPool<String> sampleTest;

    @Before
    public void runBeforeEachTest()
    {
        Builder<String> builder = new Builder<>();

        this.sampleTest = builder.addData( List.of( "a", "b", "c" ) )
                                 .setMetadata( PoolMetadata.of() )
                                 .addDataForBaseline( List.of( "d", "e", "f" ) )
                                 .setMetadataForBaseline( PoolMetadata.of() )
                                 .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                 .build();
    }

    @Test
    public void testGetRawData()
    {
        assertEquals( List.of( "a", "b", "c" ), this.sampleTest.getRawData() );
    }

    @Test
    public void testHasBaseline()
    {
        assertTrue( this.sampleTest.hasBaseline() );
    }

    @Test
    public void testGetBaselineData()
    {
        Builder<String> builder = new Builder<>();

        Pool<String> expected = builder.addData( List.of( "d", "e", "f" ) )
                                       .setMetadata( PoolMetadata.of() )
                                       .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                       .build();

        assertEquals( expected, this.sampleTest.getBaselineData() );
    }

    @Test
    public void testHasClimatology()
    {
        assertTrue( this.sampleTest.hasClimatology() );
    }

    @Test
    public void testGetClimatology()
    {
        assertEquals( VectorOfDoubles.of( 1, 2, 3 ), this.sampleTest.getClimatology() );
    }

    @Test
    public void testGetMetadata()
    {
        assertEquals( PoolMetadata.of(), this.sampleTest.getMetadata() );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertTrue( this.sampleTest.equals( this.sampleTest ) );

        // Symmetric
        Pool<String> another =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( PoolMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( PoolMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertTrue( another.equals( this.sampleTest ) && this.sampleTest.equals( another ) );

        // Transitive
        Pool<String> yetAnother =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( PoolMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( PoolMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertTrue( this.sampleTest.equals( another ) && another.equals( yetAnother )
                    && this.sampleTest.equals( yetAnother ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( this.sampleTest.equals( another ) );
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

        assertNotEquals( unequalOnData, this.sampleTest );

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

        assertNotEquals( unequalOnMetadata, this.sampleTest );

        Pool<String> unequalOnBaseline =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( PoolMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "q" ) )
                                                    .setMetadataForBaseline( PoolMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertNotEquals( unequalOnBaseline, this.sampleTest );

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

        assertNotEquals( unequalOnBaselineMeta, this.sampleTest );

        Pool<String> unequalOnClimatology =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( PoolMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( PoolMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 4 ) )
                                                    .build();

        assertNotEquals( unequalOnClimatology, this.sampleTest );
    }

    @Test
    public void testHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.sampleTest.hashCode(), this.sampleTest.hashCode() );

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
            assertEquals( this.sampleTest.hashCode(), another.hashCode() );
        }
    }


    @Test
    public void testExceptionOnConstructionWithNullEntry()
    {
        Builder<String> builder = new Builder<>();

        builder.addData( Arrays.asList( (String) null ) ).setMetadata( PoolMetadata.of() );

        assertThrows( PoolException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionWithNaNInClimatology()
    {
        VectorOfDoubles climatology = VectorOfDoubles.of( Double.NaN );

        Builder<String> builder = new Builder<>();

        builder.addData( List.of( "OK" ) )
               .setClimatology( climatology )
               .setMetadata( PoolMetadata.of() );

        assertThrows( PoolException.class, () -> builder.build() );
    }


}
