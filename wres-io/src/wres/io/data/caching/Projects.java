package wres.io.data.caching;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import static wres.io.config.LeftOrRightOrBaseline.*;

import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.LRUMap;

/**
 * Cache of available types of forecast
 */
// TODO: Find a way to remove the projects cache
@Deprecated(forRemoval = true) // We shouldn't cache; there will only ever be one project
public class Projects
{
    private static final String NEWLINE = System.lineSeparator();

    private Map<Integer, Integer> keyIndex;
    private ConcurrentMap<Integer, Project> details;

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

    final ConcurrentMap<Integer, Project> getDetails()
    {
        synchronized ( Projects.DETAIL_LOCK )
        {
            this.initializeDetails();
            return this.details;
        }
    }

    /**
     * <p>Invalidates the global cache of the singleton associated with this class, {@link #instance}.
     * 
     * <p>See #61206.
     */
    
    public static void invalidateGlobalCache()
    {
        synchronized ( CACHE_LOCK )
        {
            if ( Objects.nonNull( instance ) )
            {
                Projects.instance.details = null;
            }
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

    private Project get( Integer id)
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
    private void addElement( Project element ) throws SQLException
    {
        element.save();
        this.add(element);
    }

    private void add( Project project )
    {
        synchronized (Projects.KEY_LOCK)
        {
            this.getKeyIndex().put(project.getInputCode(), project.getId());

            if (this.details != null && !this.details.containsKey(project.getId()))
            {
                this.getDetails().put(project.getId(), project);
            }
        }
    }

    private static Pair<Project,Boolean> getProject( ProjectConfig projectConfig,
                                                     List<String> leftHashes,
                                                     List<String> rightHashes,
                                                     List<String> baselineHashes )
            throws SQLException
    {
        Project details = null;
        boolean thisCallCausedInsert = false;
        Integer inputCode = ConfigHelper.hashProject(
                projectConfig,
                leftHashes,
                rightHashes,
                baselineHashes
        );

        if (Projects.getCache().hasID( inputCode ))
        {
            LOGGER.debug( "Found project with key {} in cache.", inputCode );
            details = Projects.getCache().get( Projects.getCache().getID( inputCode )  );
        }

        if (details == null)
        {
            LOGGER.debug( "Did NOT find project with key {} in cache.",
                          inputCode );

            details = new Project( projectConfig,
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

        try
        {
            connection = Database.getHighPriorityConnection();

            try (DataProvider data = new DataScripter( "SELECT * FROM wres.Project").getData())
            {
                data.consume(
                        dataset -> {
                            Integer projectId = dataset.getInt( "project_id" );
                            Integer inputCode = dataset.getInt( "input_code" );
                            this.getKeyIndex().put(
                                    inputCode,
                                    projectId
                            );
                        }
                );
            }
            
            LOGGER.debug( "Finished populating the Projects details." );

        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Project cache.",
                         error );
        }
        finally
        {
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
    public static Project getProjectFromIngest( ProjectConfig projectConfig,
                                                List<IngestResult> ingestResults )
            throws SQLException, IOException
    {
        List<String> leftHashes = new ArrayList<>();
        List<String> rightHashes = new ArrayList<>();
        List<String> baselineHashes = new ArrayList<>();

        for ( IngestResult ingestResult : ingestResults )
        {
            int countAdded = 0;

            if ( ingestResult.getLeftOrRightOrBaseline()
                             .equals( LeftOrRightOrBaseline.LEFT ) )
            {
                leftHashes.add( ingestResult.getHash() );
                countAdded++;
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                rightHashes.add( ingestResult.getHash() );
                countAdded++;
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                baselineHashes.add( ingestResult.getHash() );
                countAdded++;
            }
            else
            {
                throw new IllegalArgumentException( "An ingest result did not "
                                                    + "have left/right/baseline"
                                                    + " associated with it: "
                                                    + ingestResult );
            }

            // Additionally include links when a source is re-used in a project.
            for ( LeftOrRightOrBaseline leftOrRightOrBaseline :
                    ingestResult.getDataSource()
                                .getLinks() )
            {
                if ( leftOrRightOrBaseline.equals( LEFT ) )
                {
                    leftHashes.add( ingestResult.getHash() );
                    countAdded++;
                }
                else if ( leftOrRightOrBaseline.equals( RIGHT ) )
                {
                    rightHashes.add( ingestResult.getHash() );
                    countAdded++;
                }
                else if ( leftOrRightOrBaseline.equals( BASELINE ) )
                {
                    baselineHashes.add( ingestResult.getHash() );
                    countAdded++;
                }
            }

            LOGGER.debug( "Noticed {} has {} links.", ingestResult, countAdded );
        }

        List<String> finalLeftHashes = Collections.unmodifiableList( leftHashes );
        List<String> finalRightHashes = Collections.unmodifiableList( rightHashes );
        List<String> finalBaselineHashes = Collections.unmodifiableList( baselineHashes );

        // Check assumption that at least one left and one right source have
        // been created.
        int leftCount = finalLeftHashes.size();
        int rightCount = finalRightHashes.size();

        if ( leftCount < 1 || rightCount < 1 )
        {
            throw new IllegalStateException( "At least one source for left and "
                                             + "one source for right must be "
                                             + "linked, but left had "
                                             + leftCount + " sources and right "
                                             + "had " + rightCount
                                             + " sources." );
        }

        Pair<Project,Boolean> detailsResult =
                Projects.getProject( projectConfig,
                                     finalLeftHashes,
                                     finalRightHashes,
                                     finalBaselineHashes );
        Project details = detailsResult.getLeft();
        int detailsId = details.getId();

        if ( detailsResult.getRight() )
        {
            LOGGER.debug( "Found that this Thread is responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );

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
                    throw new IngestException( "The id for source data '"
                                               + ingestResult.getHash()
                                               + "' that must be linked to "
                                               + "project "
                                               + details.getId()
                                               + " could not be determined. The"
                                               + " data ingest cannot "
                                               + "continue.");
                }

                copyStatement.add( details.getId() + delimiter
                                   + sourceID + delimiter
                                   + ingestResult.getLeftOrRightOrBaseline().value() );

                for ( LeftOrRightOrBaseline additionalLink :
                        ingestResult.getDataSource().getLinks() )
                {
                    copyStatement.add( details.getId() + delimiter
                                       + sourceID + delimiter
                                       + additionalLink.value() );
                }
            }

            String allCopyValues = copyStatement.toString();
            LOGGER.trace( "Full copy statement: {}", allCopyValues );
            Database.copy( copyHeader, allCopyValues, delimiter );
        }
        else
        {
            LOGGER.debug( "Found that this Thread is NOT responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );
            DataScripter scripter = new DataScripter();
            scripter.addLine( "SELECT COUNT( source_id )" );
            scripter.addLine( "FROM wres.ProjectSource" );
            scripter.addLine( "WHERE project_id = ?" );
            scripter.addArgument( detailsId );

            // Need to wait here until the data is available. How long to wait?
            // Start with 30ish seconds, error out after that. We might actually
            // wait longer than 30 seconds.
            long startMillis = System.currentTimeMillis();
            long endMillis = startMillis + Duration.ofSeconds( 30 )
                                                   .toMillis();
            long currentMillis = startMillis;
            long sleepMillis = Duration.ofSeconds( 1 )
                                       .toMillis();
            boolean projectSourceRowsFound = false;

            while ( currentMillis < endMillis )
            {
                try ( DataProvider dataProvider = scripter.getData() )
                {
                    long count = dataProvider.getLong( "count" );

                    if ( count > 1 )
                    {
                        // We assume that the projectsource rows are made
                        // in a single transaction here. We further assume that
                        // each project will have at least a left and right
                        // member, therefore 2 or more rows (greater than 1).
                        LOGGER.debug( "wres.ProjectSource rows present for {}",
                                      detailsId );
                        projectSourceRowsFound = true;
                        break;
                    }
                    else
                    {
                        LOGGER.debug( "wres.ProjectSource rows missing for {}",
                                      detailsId );
                        Thread.sleep( sleepMillis );
                    }
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while waiting for wres.ProjectSource rows.", ie );
                    Thread.currentThread().interrupt();
                    // No need to rethrow, the evaluation will fail.
                }

                currentMillis = System.currentTimeMillis();
            }

            if ( !projectSourceRowsFound )
            {
                throw new IngestException( "Another WRES instance failed to "
                                           + "complete ingest that this "
                                           + "evaluation depends on." );
            }
        }

        return details;
    }
}
