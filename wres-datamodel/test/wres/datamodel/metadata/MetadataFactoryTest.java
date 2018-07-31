package wres.datamodel.metadata;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;

import org.junit.Test;

import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataException;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.ReferenceTime;

/**
 * Tests the {@link DefaultMetadataFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetadataFactoryTest
{

    /**
     * Tests the {@link MetadataFactory#unionOf(java.util.List)} against a benchmark.
     */
    @Test
    public void unionOf()
    {
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m1 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                           MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" ),
                                           TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1985-12-31T23:59:59Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m2 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                           MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" ),
                                           TimeWindow.of( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-12-31T23:59:59Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m3 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                           MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" ),
                                           TimeWindow.of( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1988-01-01T00:00:00Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location benchmarkLocation = MetadataFactory.getLocation( "DRRC2" );
        Metadata benchmark = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                  MetadataFactory.getDatasetIdentifier( benchmarkLocation, "SQIN", "HEFS" ),
                                                          TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                        Instant.parse( "1988-01-01T00:00:00Z" ),
                                                                        ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Unexpected difference between union of metadata and benchmark.",
                    benchmark.equals( MetadataFactory.unionOf( Arrays.asList( m1, m2, m3 ) ) ) );
        //Checked exception
        try
        {
            Location failLocation = MetadataFactory.getLocation( "DRRC3" );
            Metadata fail = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                 MetadataFactory.getDatasetIdentifier( failLocation, "SQIN", "HEFS" ) );
            MetadataFactory.unionOf( Arrays.asList( m1, m2, m3, fail ) );
            fail( "Expected a checked exception on building the union of metadata for unequal inputs." );
        }
        catch ( MetadataException e )
        {
        }
    }

}
