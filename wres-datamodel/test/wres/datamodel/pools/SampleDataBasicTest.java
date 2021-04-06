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
import wres.datamodel.pools.SampleDataBasic.SampleDataBasicBuilder;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link SampleDataBasic}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SampleDataBasicTest
{

    /**
     * An instance for testing.
     */

    private SampleDataBasic<String> sampleTest;

    @Before
    public void runBeforeEachTest()
    {
        SampleDataBasicBuilder<String> builder = new SampleDataBasicBuilder<>();

        this.sampleTest = builder.addData( List.of( "a", "b", "c" ) )
                                 .setMetadata( SampleMetadata.of() )
                                 .addDataForBaseline( List.of( "d", "e", "f" ) )
                                 .setMetadataForBaseline( SampleMetadata.of() )
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
        SampleDataBasicBuilder<String> builder = new SampleDataBasicBuilder<>();

        SampleData<String> expected = builder.addData( List.of( "d", "e", "f" ) )
                                             .setMetadata( SampleMetadata.of() )
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
        assertEquals( SampleMetadata.of(), this.sampleTest.getMetadata() );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertTrue( this.sampleTest.equals( this.sampleTest ) );

        // Symmetric
        SampleData<String> another =
                new SampleDataBasicBuilder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertTrue( another.equals( this.sampleTest ) && this.sampleTest.equals( another ) );

        // Transitive
        SampleData<String> yetAnother =
                new SampleDataBasicBuilder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of() )
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
        SampleData<String> unequalOnData =
                new SampleDataBasicBuilder<String>().addData( List.of( "z", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertNotEquals( unequalOnData, this.sampleTest );

        SampleData<String> unequalOnMetadata =
                new SampleDataBasicBuilder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of( Evaluation.newBuilder()
                                                                                               .setMeasurementUnit( "CFS" )
                                                                                               .build(),
                                                                                     Pool.getDefaultInstance() ) )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertNotEquals( unequalOnMetadata, this.sampleTest );

        SampleData<String> unequalOnBaseline =
                new SampleDataBasicBuilder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "q" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of() )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertNotEquals( unequalOnBaseline, this.sampleTest );

        SampleData<String> unequalOnBaselineMeta =
                new SampleDataBasicBuilder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of( Evaluation.newBuilder()
                                                                                                          .setMeasurementUnit( "CFS" )
                                                                                                          .build(),
                                                                                                Pool.getDefaultInstance() ) )
                                                    .setClimatology( VectorOfDoubles.of( 1, 2, 3 ) )
                                                    .build();

        assertNotEquals( unequalOnBaselineMeta, this.sampleTest );

        SampleData<String> unequalOnClimatology =
                new SampleDataBasicBuilder<String>().addData( List.of( "a", "b", "c" ) )
                                                    .setMetadata( SampleMetadata.of() )
                                                    .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                    .setMetadataForBaseline( SampleMetadata.of() )
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
        SampleDataBasicBuilder<String> builder = new SampleDataBasicBuilder<>();

        SampleData<String> another = builder.addData( List.of( "a", "b", "c" ) )
                                            .setMetadata( SampleMetadata.of() )
                                            .addDataForBaseline( List.of( "d", "e", "f" ) )
                                            .setMetadataForBaseline( SampleMetadata.of() )
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
        SampleDataBasicBuilder<String> builder = new SampleDataBasicBuilder<>();

        builder.addData( Arrays.asList( (String) null ) ).setMetadata( SampleMetadata.of() );

        assertThrows( SampleDataException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionWithNaNInClimatology()
    {
        VectorOfDoubles climatology = VectorOfDoubles.of( Double.NaN );

        SampleDataBasicBuilder<String> builder = new SampleDataBasicBuilder<>();

        builder.addData( List.of( "OK" ) )
               .setClimatology( climatology )
               .setMetadata( SampleMetadata.of() );

        assertThrows( SampleDataException.class, () -> builder.build() );
    }


}
