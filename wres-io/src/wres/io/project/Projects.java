package wres.io.project;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.EvaluationDeclaration;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.database.caching.DatabaseCaches;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.io.database.Database;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;

/**
 * Factory class for creating various implementations of a {@link Project}, such as an in-memory project and a project
 * backed by a database.
 */
public class Projects
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Projects.class );

    /**
     * Creates a {@link Project} backed by a database.
     * @param database The database to use
     * @param declaration the project declaration
     * @param caches the database caches/ORMs
     * @param griddedFeatures the gridded features cache, if required
     * @param ingestResults the ingest results
     * @return the project
     * @throws IllegalStateException when another process already holds lock
     * @throws NullPointerException if any input is null
     * @throws IngestException when anything else goes wrong
     */
    public static Project getProject( Database database,
                                      EvaluationDeclaration declaration,
                                      DatabaseCaches caches,
                                      GriddedFeatures griddedFeatures,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( ingestResults );

        try
        {
            LOGGER.debug( "Creating a database project." );

            return new DatabaseProject( database,
                                        caches,
                                        griddedFeatures,
                                        declaration,
                                        ingestResults );
        }
        catch ( SQLException | IngestException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }
    }

    /**
     * Creates a {@link Project} backed by an in-memory {@link TimeSeriesStore}.
     * @param declaration the project declaration
     * @param timeSeriesStore the store of time-series data
     * @param ingestResults the ingest results
     * @return the project
     */
    public static Project getProject( EvaluationDeclaration declaration,
                                      TimeSeriesStore timeSeriesStore,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( timeSeriesStore );
        Objects.requireNonNull( ingestResults );

        LOGGER.debug( "Creating an in-memory project." );

        return new InMemoryProject( declaration, timeSeriesStore, ingestResults );
    }

    /**
     * Do not construct.
     */
    private Projects()
    {
    }
}
