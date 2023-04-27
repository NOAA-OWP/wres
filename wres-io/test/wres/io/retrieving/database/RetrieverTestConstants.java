package wres.io.retrieving.database;

import java.net.URI;
import java.time.Instant;

import wres.datamodel.space.Feature;
import wres.statistics.MessageFactory;

class RetrieverTestConstants
{
    /** A project surrogate id for testing */
    static final long PROJECT_ID = 1;

    /** A project natural id (hash) for testing */
    static final String PROJECT_HASH = "123deadbeef456";


    static final Feature FEATURE = Feature.of(
            MessageFactory.getGeometry( "F" ) );
    static final String VARIABLE_NAME = "Q";

    static final String UNIT = "[ft_i]3/s";

    static final URI FAKE_URI = URI.create( "file:///some.csv" );
    static final Instant T2023_04_01T00_00_00Z = Instant.parse( "2023-04-01T00:00:00Z" );
    static final Instant T2023_04_01T01_00_00Z = Instant.parse( "2023-04-01T01:00:00Z" );
    static final Instant T2023_04_01T03_00_00Z = Instant.parse( "2023-04-01T03:00:00Z" );
    static final Instant T2023_04_01T04_00_00Z = Instant.parse( "2023-04-01T04:00:00Z" );
    static final Instant T2023_04_01T06_00_00Z = Instant.parse( "2023-04-01T06:00:00Z" );
    static final Instant T2023_04_01T07_00_00Z = Instant.parse( "2023-04-01T07:00:00Z" );
    static final Instant T2023_04_01T19_00_00Z = Instant.parse( "2023-04-01T19:00:00Z" );
    static final Instant T2023_04_01T17_00_00Z = Instant.parse( "2023-04-01T17:00:00Z" );

    private RetrieverTestConstants()
    {
        // Disallow construction, only here for the constants
    }
}
