package wres.datamodel.pools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.types.Climatology;
import wres.datamodel.pools.Pool.Builder;
import wres.datamodel.space.Feature;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;

/**
 * Tests the {@link Pool}.
 *  
 * @author James Brown
 */

class PoolTest
{
    /** An instance for testing. */
    private Pool<String> testPool;

    /** Instance of climatology for testing.*/
    private Climatology climatology;

    /** Another instance of climatology for testing.*/
    private Climatology anotherClimatology;

    @BeforeEach
    void runBeforeEachTest()
    {
        Builder<String> builder = new Builder<>();

        Geometry geometry = Geometry.newBuilder()
                                    .setName( "feature" )
                                    .build();
        Feature feature = Feature.of( geometry );

        this.climatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3 }, "bar" )
                                         .build();

        this.anotherClimatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 4, 5, 6 }, "bar" )
                                         .build();

        this.testPool = builder.addData( List.of( "a", "b", "c" ) )
                               .setMetadata( PoolMetadata.of() )
                               .addDataForBaseline( List.of( "d", "e", "f" ) )
                               .setMetadataForBaseline( PoolMetadata.of( true ) )
                               .setClimatology( this.climatology )
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
                                          PoolMetadata.of( true ),
                                          this.climatology );
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
                                       .setMetadata( PoolMetadata.of( true ) )
                                       .setClimatology( this.climatology )
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
        Geometry geometry = Geometry.newBuilder()
                                    .setName( "feature" )
                                    .build();
        Feature feature = Feature.of( geometry );

        Climatology expected =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3 }, "bar" )
                                         .build();

        assertEquals( expected, this.testPool.getClimatology() );
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
        assertEquals( this.testPool, this.testPool );

        // Symmetric
        Pool<String> another =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( "d" )
                                     .addDataForBaseline( "e" )
                                     .addDataForBaseline( "f" )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .setClimatology( this.climatology )
                                     .build();

        assertTrue( another.equals( this.testPool ) && this.testPool.equals( another ) );

        // Transitive
        Pool<String> yetAnother =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .setClimatology( this.climatology )
                                     .build();

        assertTrue( this.testPool.equals( another ) && another.equals( yetAnother )
                    && this.testPool.equals( yetAnother ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testPool, another );
        }

        // Nullity
        assertNotEquals( null, another );
        assertNotEquals( null, another );

        // Check unequal cases
        Pool<String> unequalOnData =
                new Builder<String>().addData( List.of( "z", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .setClimatology( this.climatology )
                                     .build();

        assertNotEquals( unequalOnData, this.testPool );

        Pool<String> unequalOnMetadata =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of( Evaluation.newBuilder()
                                                                              .setMeasurementUnit( "CFS" )
                                                                              .build(),
                                                                    wres.statistics.generated.Pool.getDefaultInstance() ) )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .setClimatology( this.climatology )
                                     .build();

        assertNotEquals( unequalOnMetadata, this.testPool );

        Pool<String> unequalOnBaseline =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "q" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .setClimatology( this.climatology )
                                     .build();

        assertNotEquals( unequalOnBaseline, this.testPool );

        Pool<String> unequalOnBaselineMeta =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( Evaluation.newBuilder()
                                                                                         .setMeasurementUnit( "CFS" )
                                                                                         .build(),
                                                                               wres.statistics.generated.Pool.newBuilder()
                                                                                                             .setIsBaselinePool( true )
                                                                                                             .build() ) )
                                     .setClimatology( this.climatology )
                                     .build();

        assertNotEquals( unequalOnBaselineMeta, this.testPool );

        Pool<String> unequalOnClimatology =
                new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .setClimatology( this.anotherClimatology )
                                     .build();

        assertNotEquals( unequalOnClimatology, this.testPool );

        Pool<String> noBaseline = new Pool.Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                            .setMetadata( PoolMetadata.of() )
                                                            .setClimatology( this.climatology )
                                                            .build();
        assertNotEquals( this.testPool, noBaseline );

        Pool<String> noClimatology = new Pool.Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                               .setMetadata( PoolMetadata.of() )
                                                               .addDataForBaseline( List.of( "d", "e", "f" ) )
                                                               .setMetadataForBaseline( PoolMetadata.of( true ) )
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
                                      .setMetadataForBaseline( PoolMetadata.of( true ) )
                                      .setClimatology( this.climatology )
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
                                                        .setMetadataForBaseline( PoolMetadata.of( true ) )
                                                        .build();

        Pool<String> actual = new Builder<String>().addPool( this.testPool )
                                                   .addPool( anotherPool )
                                                   .build();

        Pool<String> expected =
                new Builder<String>().addData( List.of( "a", "b", "c", "d", "e", "f" ) )
                                     .setMetadata( PoolMetadata.of() )
                                     .addDataForBaseline( List.of( "d", "e", "f", "a", "b", "c" ) )
                                     .setMetadataForBaseline( PoolMetadata.of( true ) )
                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGetMiniPools()
    {
        assertEquals( Collections.singletonList( this.testPool ), this.testPool.getMiniPools() );

        Pool<String> anotherPool = new Builder<String>().addData( List.of( "d", "e", "f" ) )
                                                        .setMetadata( PoolMetadata.of() )
                                                        .addDataForBaseline( List.of( "a", "b", "c" ) )
                                                        .setMetadataForBaseline( PoolMetadata.of( true ) )
                                                        .setClimatology( this.anotherClimatology )
                                                        .build();

        Pool<String> merged = new Builder<String>().addPool( this.testPool )
                                                   .addPool( anotherPool )
                                                   .build();

        List<Pool<String>> actual = merged.getMiniPools();
        List<Pool<String>> expected = List.of( this.testPool, anotherPool );

        assertEquals( expected, actual );
    }

    @Test
    void testGetBaselinePreservesMiniPools()
    {
        assertEquals( Collections.singletonList( this.testPool ), this.testPool.getMiniPools() );

        Pool<String> anotherPool = new Builder<String>().addData( List.of( "d", "e", "f" ) )
                                                        .setMetadata( PoolMetadata.of() )
                                                        .addDataForBaseline( List.of( "a", "b", "c" ) )
                                                        .setMetadataForBaseline( PoolMetadata.of( true ) )
                                                        .setClimatology( this.anotherClimatology )
                                                        .build();

        Pool<String> merged = new Builder<String>().addPool( this.testPool )
                                                   .addPool( anotherPool )
                                                   .build();

        List<Pool<String>> actual = merged.getBaselineData().getMiniPools();
        List<Pool<String>> expected = List.of( this.testPool.getBaselineData(), anotherPool.getBaselineData() );

        assertEquals( expected, actual );
    }

    @Test
    void testToString()
    {
        String actual = this.testPool.toString();
        String expected = "Pool[mainMetadata=PoolMetadata[poolId=0,leftDataName=,rightDataName=,baselineDataName=,"
                          + "leftVariableName=,rightVariableName=,baselineVariableName=,isBaselinePool=false,"
                          + "features=<null>,timeWindow=<null>,thresholds=<null>,timeScale=<null>,measurementUnit="
                          + "DIMENSIONLESS,ensembleAverageType=NONE],mainData=[a, b, c],baselineMetadata="
                          + "PoolMetadata[poolId=0,leftDataName=,rightDataName=,baselineDataName=,leftVariableName=,"
                          + "rightVariableName=,baselineVariableName=,isBaselinePool=true,features=<null>,"
                          + "timeWindow=<null>,thresholds=<null>,timeScale=<null>,measurementUnit=DIMENSIONLESS,"
                          + "ensembleAverageType=NONE],baselineData=[d, e, f],climatology=Climatology[byFeature="
                          + "{Feature[name=feature,description=,srid=0,wkt=]=[1.0, 2.0, 3.0]}]]";

        assertEquals( expected, actual );
    }

    @Test
    void testExceptionOnConstructionWithNullEntry()
    {
        Builder<String> builder = new Builder<>();

        builder.addData( Collections.singletonList( null ) ).setMetadata( PoolMetadata.of() );

        assertThrows( NullPointerException.class, builder::build );
    }

    @Test
    void testExceptionOnConstructionWithNullEntryInBaseline()
    {
        Builder<String> builder = new Builder<String>().addData( "Foo" )
                                                       .setMetadata( PoolMetadata.of() )
                                                       .addDataForBaseline( Collections.singletonList( null ) )
                                                       .setMetadataForBaseline( PoolMetadata.of() );

        assertThrows( NullPointerException.class, builder::build );
    }

    @Test
    void testExceptionOnConstructionWithoutMetadata()
    {
        List<String> list = List.of( "OK" );
        Builder<String> builder = new Builder<String>().addData( list );
        assertThrows( PoolException.class, builder::build );
    }

    @Test
    void testExceptionOnConstructionWithoutBaselineMetadata()
    {
        Builder<String> builder = new Builder<String>().addData( "Foo" )
                                                       .setMetadata( PoolMetadata.of() )
                                                       .addDataForBaseline( "Bar" );

        assertThrows( PoolException.class, builder::build );
    }
}
