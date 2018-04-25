package wres.io.data.caching;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.io.utilities.LRUMap;

/**
 * Cache of available types of forecast
 */
// TODO: Find a way to remove the projects cache
@Deprecated // We shouldn't cache; there will only ever be one project
public class Projects
{
    private static final String NEWLINE = System.lineSeparator();

    private Map<Integer, Integer> keyIndex;
    private ConcurrentMap<Integer, ProjectDetails> details;

    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);
    private static Projects instance = null;
    private static final Object CACHE_LOCK = new Object();

    private static final Object DETAIL_LOCK = new Object();
    private static final Object KEY_LOCK = new Object();

    private static Projects getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if ( instance == null)
            {
                instance = new Projects();
                instance.init();
            }
            return instance;
        }
    }

    private Map<Integer, Integer> getKeyIndex()
    {
        synchronized ( Projects.KEY_LOCK )
        {
            if ( keyIndex == null )
            {
                keyIndex = new LRUMap<>( this.getMaxDetails(), eldest -> {
                    if ( this.details != null )
                    {
                        details.remove( eldest.getValue() );
                    }
                } );
            }
        }

        return this.keyIndex;
    }

    final ConcurrentMap<Integer, ProjectDetails> getDetails()
    {
        synchronized ( Projects.DETAIL_LOCK )
        {
            this.initializeDetails();
            return this.details;
        }
    }

    private void initializeDetails()
    {
        synchronized ( Projects.DETAIL_LOCK )
        {
            if (this.details == null)
            {
                this.details = new ConcurrentHashMap<>( this.getMaxDetails() );
            }
        }
    }

    private boolean hasID(Integer key)
    {
        boolean hasIt;

        synchronized ( Projects.KEY_LOCK )
        {
            hasIt = this.getKeyIndex().containsKey(key);
        }

        return hasIt;
    }

    private Integer getID(Integer key)
    {
        Integer id = null;

        synchronized (Projects.KEY_LOCK)
        {
            if (this.getKeyIndex().containsKey(key))
            {
                id = this.getKeyIndex().get(key);
            }
        }

        return id;
    }

    private ProjectDetails get(Integer id)
    {
        return this.getDetails().get(id);
    }

    /**
     * Adds the details to the instance cache. If the details don't exist in the database, they are added.
     * <br><br>
     * Since only a limited amount of data is stored within the instanced cache, the least recently used item from the
     * instanced cache is removed if the amount surpasses the maximum allowable number of stored details
     * @param element The details to add to the instanced cache
     * @throws SQLException Thrown if the ID of the element could not be retrieved or the cache could not be
     * updated
     */
    private void addElement( ProjectDetails element ) throws SQLException
    {
        element.save();
        this.add(element);
    }

    private void add( ProjectDetails element )
    {
        synchronized (Projects.KEY_LOCK)
        {
            this.getKeyIndex().put(element.getKey(), element.getId());

            if (this.details != null && !this.details.containsKey(element.getId()))
            {
                this.getDetails().put(element.getId(), element);
            }
        }
    }

    private static Pair<ProjectDetails,Boolean> getProject( ProjectConfig projectConfig,
                                                           List<String> leftHashes,
                                                           List<String> rightHashes,
                                                           List<String> baselineHashes )
            throws SQLException
    {
        ProjectDetails details = null;
        boolean thisCallCausedInsert = false;
        Integer inputCode = ProjectDetails.hash( projectConfig,
                                                 leftHashes,
                                                 rightHashes,
                                                 baselineHashes );

        if (Projects.getCache().hasID( inputCode ))
        {
            LOGGER.debug( "Found project with key {} in cache.", inputCode );
            details = Projects.getCache().get( Projects.getCache().getID( inputCode )  );
        }

        if (details == null)
        {
            LOGGER.debug( "Did NOT find project with key {} in cache.",
                          inputCode );

            details = new ProjectDetails( projectConfig,
                                          inputCode );

            // Caller cannot trust the boolean flag on the details since we are
            // caching the exact details object. Only the creator of the details
            // can reliably say "hey I was the one who caused this row to be
            // inserted."
            Projects.getCache().addElement( details );

            // addElement will call .save() on the details, which then allows us
            // to interrogate the same details object we passed in. Maybe this
            // should be more direct.
            thisCallCausedInsert = details.performedInsert();
            LOGGER.debug( "Did the ProjectDetails created by this Thread insert into the database first? {}",
                          thisCallCausedInsert );
        }


        return Pair.of( details, thisCallCausedInsert );
    }

    private int getMaxDetails() {
        return 10;
    }

    private void init()
    {
        this.initializeDetails();

        Connection connection = null;
        ResultSet projects = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT * FROM wres.project;";

            projects = Database.getResults(connection, loadScript);

            while (projects.next())
            {
                Integer projectId = projects.getInt( "project_id" );
                Integer inputCode = projects.getInt("input_code");
                this.getKeyIndex().put(
                        inputCode,
                        projectId
                );
            }

        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Project cache.",
                         error );
        }
        finally
        {
            if (projects != null)
            {
                try
                {
                    projects.close();
                }
                catch (SQLException error)
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "The result set {} containing projects could not be closed.",
                                 projects, error );
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }

    /**
     * Convert a projectConfig and a raw list of IngestResult into ProjectDetails
     * @param projectConfig the config that produced the ingest results
     * @param ingestResults the ingest results
     * @return the ProjectDetails to use
     * @throws SQLException when ProjectDetails construction goes wrong
     * @throws IllegalArgumentException when an IngestResult does not have left/right/baseline information
     * @throws IOException when a source identifier cannot be determined
     */
    public static ProjectDetails getProjectFromIngest( ProjectConfig projectConfig,
                                                       List<IngestResult> ingestResults )
            throws SQLException, IOException
    {
        List<String> leftHashes = new ArrayList<>();
        List<String> rightHashes = new ArrayList<>();
        List<String> baselineHashes = new ArrayList<>();

        for ( IngestResult ingestResult : ingestResults )
        {
            if ( ingestResult.getLeftOrRightOrBaseline()
                             .equals( LeftOrRightOrBaseline.LEFT ) )
            {
                leftHashes.add( ingestResult.getHash() );
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                rightHashes.add( ingestResult.getHash() );
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                baselineHashes.add( ingestResult.getHash() );
            }
            else
            {
                throw new IllegalArgumentException( "An ingest result did not "
                                                    + "have left/right/baseline"
                                                    + " associated with it: "
                                                    + ingestResult );
            }
        }

        List<String> finalLeftHashes = Collections.unmodifiableList( leftHashes );
        List<String> finalRightHashes = Collections.unmodifiableList( rightHashes );
        List<String> finalBaselineHashes = Collections.unmodifiableList( baselineHashes );
        Pair<ProjectDetails,Boolean> detailsResult =
                Projects.getProject( projectConfig,
                                     finalLeftHashes,
                                     finalRightHashes,
                                     finalBaselineHashes );
        ProjectDetails details = detailsResult.getLeft();

        if ( detailsResult.getRight() )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Found that this Thread is responsible for "
                              + "wres.ProjectSource rows for project {}",
                              details.getId() );
            }

            // If we just created the Project, we are responsible for relating
            // project to source. Otherwise we trust it is present.
            String copyHeader = "wres.ProjectSource (project_id, source_id, member)";
            String delimiter = "|";
            StringJoiner copyStatement = new StringJoiner( NEWLINE );

            for ( IngestResult ingestResult : ingestResults )
            {
                Integer sourceID =
                    DataSources.getActiveSourceID( ingestResult.getHash() );

                if (sourceID == null)
                {
                    throw new IOException( "The id for a source file that must "
                                           + "be linked to this project could "
                                           + "not be determined. The data "
                                           + "ingest cannot continue." );
                }

                copyStatement.add( details.getId() + delimiter
                                   + sourceID + delimiter
                                   + ingestResult.getLeftOrRightOrBaseline().value() );
            }

            String allCopyValues = copyStatement.toString();
            LOGGER.trace( "Full copy statement: {}", allCopyValues );
            Database.copy( copyHeader, allCopyValues, delimiter );
        }
        else if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Found that this Thread is NOT responsible for "
                          + "wres.ProjectSource rows for project {}",
                          details.getId() );
        }

        return details;
    }
}
