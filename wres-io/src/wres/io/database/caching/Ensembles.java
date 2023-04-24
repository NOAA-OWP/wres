package wres.io.database.caching;

import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.io.data.DataProvider;
import wres.io.database.details.EnsembleDetails;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.retrieving.DataAccessException;

/**
 * Cached details about Ensembles from the database
 * @author James Brown
 * @author Christopher Tubbs
 */
public class Ensembles
{
    private static final int MAX_DETAILS = 500;

    private static final Logger LOGGER = LoggerFactory.getLogger( Ensembles.class );

    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long, EnsembleDetails> keyToValue = Caffeine.newBuilder()
                                                                    .maximumSize( MAX_DETAILS )
                                                                    .build();
    private final Cache<String, Long> valueToKey = Caffeine.newBuilder()
                                                           .maximumSize( MAX_DETAILS )
                                                           .build();

    /** Lock object to minimize round-trips to the database when populating the ensembles cache. */
    private final Object lock = new Object();

    /**
     * Creates an instance.
     * @param database the database
     * @throws NullPointerException if the database is null
     */
    public Ensembles( Database database )
    {
        Objects.requireNonNull( database );

        this.database = database;
    }

    /**
     * Mark this instance as only being allowed to read from the database, in other words, not being allowed to add new 
     * features, but allowed to look for existing features. During ingest, read and create. During retrieval, read only.
     */

    public void setOnlyReadFromDatabase()
    {
        this.onlyReadFromDatabase = true;
    }

    /**
     * Returns the ID of an Ensemble trace from the global cache based on its name
     * @param name The name of the Ensemble trace to retrieve
     * @return The surrogate key, database ID of the Ensemble
     * @throws SQLException Thrown if the ID could not be retrieved from the database
     */

    public Long getOrCreateEnsembleId( String name ) throws SQLException
    {
        if ( this.onlyReadFromDatabase )
        {
            throw new IllegalStateException( "This instance now allows no new features, call another method!" );
        }

        // If there are no identifiers...
        if ( Objects.isNull( name ) )
        {
            LOGGER.info( "When attempting to getOrCreateEnsembleId, discovered a null ensemble name." );

            // Return the default ID
            return this.getDefaultEnsembleId();
        }

        Long id = this.valueToKey.getIfPresent( name );

        if ( Objects.isNull( id ) )
        {
            synchronized ( this.lock )
            {
                // Check one more time before attempting to save. Again, this is purely to minimize db checks
                Long anotherCheck = this.valueToKey.getIfPresent( name );

                if ( Objects.nonNull( anotherCheck ) )
                {
                    return anotherCheck;
                }

                LOGGER.debug( "When attempting to getOrCreateEnsembleId, failed to discover an identifier "
                              + "corresponding to {} in the cache. Adding to the database.",
                              name );

                EnsembleDetails ensembleDetails = new EnsembleDetails( name );
                ensembleDetails.save( this.getDatabase() );
                id = ensembleDetails.getId();

                if ( Objects.isNull( id ) )
                {
                    throw new IllegalStateException( "Failed to acquire an ensemble identifier for name " + name
                                                     + "." );
                }

                this.valueToKey.put( name, id );
                this.keyToValue.put( id, ensembleDetails );
            }
        }

        return id;
    }

    /**
     * Returns the name of an ensemble trace from the global cache based on its identifier
     * @param ensembleId The ensemble identifier
     * @return The name
     * @throws SQLException Thrown if the ID could not be retrieved from the database
     */

    public String getEnsembleName( long ensembleId ) throws SQLException
    {
        EnsembleDetails ensembleDetails = this.keyToValue.getIfPresent( ensembleId );

        if ( Objects.nonNull( ensembleDetails ) )
        {
            return ensembleDetails.getKey();
        }

        String name;

        try
        {
            synchronized ( this.lock )
            {
                // Check one more time before attempting to save. Again, this is purely to minimize db checks
                EnsembleDetails anotherCheck = this.keyToValue.getIfPresent( ensembleId );

                if ( Objects.nonNull( anotherCheck ) )
                {
                    return anotherCheck.getKey();
                }

                LOGGER.debug( "Getting ensemble name for ensembleId {}.", ensembleId );

                Database innerDatabase = this.getDatabase();
                DataScripter dataScripter = new DataScripter( innerDatabase );
                dataScripter.setHighPriority( true );

                dataScripter.addLine( "SELECT ensemble_name" );
                dataScripter.addLine( "FROM wres.Ensemble" );
                dataScripter.addLine( "WHERE ensemble_id = ?" );
                dataScripter.addArgument( ensembleId );
                dataScripter.setMaxRows( 1 );
                dataScripter.setUseTransaction( false );
                dataScripter.setHighPriority( true );

                try ( DataProvider data = dataScripter.getData() )
                {
                    name = data.getString( "ensemble_name" );
                }

                // Add to cache
                ensembleDetails = new EnsembleDetails( name );
                this.valueToKey.put( name, ensembleId );
                this.keyToValue.put( ensembleId, ensembleDetails );
            }
        }
        catch ( SQLException error )
        {
            throw new DataAccessException( "Could not acquire the ensemble name for id " + ensembleId + "." );
        }

        return name;
    }

    /**
     * @return a default ensemble identifier
     * @throws SQLException if the identifier could not be retrieved or created
     */

    public Long getDefaultEnsembleId() throws SQLException
    {
        return this.getOrCreateEnsembleId( "default" );
    }

    /**
     * @return the database
     */
    private Database getDatabase()
    {
        return this.database;
    }
}
