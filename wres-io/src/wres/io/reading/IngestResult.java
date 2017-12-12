package wres.io.reading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;

/**
 * High-level result for a single fragment of ingest.
 * Multiple fragments can be used in a single source collection, and one can
 * be used multiple times in a single source collection (witness scenario400)
 */

public class IngestResult
{
    private final LeftOrRightOrBaseline leftOrRightOrBaseline;
    private final String hash;
    private final boolean foundAlready;

    private IngestResult( LeftOrRightOrBaseline leftOrRightOrBaseline,
                          String hash,
                          boolean foundAlready )
    {
        this.leftOrRightOrBaseline = leftOrRightOrBaseline;
        this.hash = hash;
        this.foundAlready = foundAlready;
    }

    public static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                   String hash,
                                   boolean foundAlready )
    {
        return new IngestResult( leftOrRightOrBaseline, hash, foundAlready );
    }

    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSourceConfig the config element ingesting for
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @return the IngestResult
     */
    public static IngestResult from( ProjectConfig projectConfig,
                                     DataSourceConfig dataSourceConfig,
                                     String hash,
                                     boolean foundAlready )
    {
        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSourceConfig );
        return IngestResult.of( leftOrRightOrBaseline,
                                hash,
                                foundAlready );
    }


    /**
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSourceConfig the config element ingesting for
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @return a list with a single IngestResult in it
     */

    public static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                         DataSourceConfig dataSourceConfig,
                                                         String hash,
                                                         boolean foundAlready )
    {
        List<IngestResult> result = new ArrayList<>( 1 );

        IngestResult ingestResult = IngestResult.from( projectConfig,
                                                       dataSourceConfig,
                                                       hash,
                                                       foundAlready );
        result.add( ingestResult );

        return Collections.unmodifiableList( result );
    }

    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.leftOrRightOrBaseline;
    }

    public String getHash()
    {
        return this.hash;
    }

    public boolean wasFoundAlready()
    {
        return this.foundAlready;
    }

    @Override
    public String toString()
    {
        return "hash: " + this.getHash() + ", "
               + "db cache hit? " + this.wasFoundAlready() + ", "
               + "l/r/b: " + getLeftOrRightOrBaseline().value();
    }
}