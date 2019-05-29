package wres.io.reading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.IngestSaver;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * Evaluates datasources specified within a project configuration and determines
 * what data should be ingested. Asynchronous tasks for each file needed for
 * ingest are created and sent to the Exector for ingestion.
 * @author Christopher Tubbs
 */
public class SourceLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);

    /**
     * The project configuration indicating what data to use
     */
    private final ProjectConfig projectConfig;
    private final DatabaseLockManager lockManager;

    /**
     * @param projectConfig the project configuration
     * @param lockManager the tool to manage ingest locks, shared per ingest
     */
    public SourceLoader( ProjectConfig projectConfig,
                         DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.lockManager = lockManager;
    }

    /**
     * Ingest data
     * @return List of Future file ingest results
     * @throws IOException when no data is found
     * @throws IngestException when getting project details fails
     */
    public List<Future<List<IngestResult>>> load() throws IOException
    {
        LOGGER.info( "Parsing files. Only {} files may be parsed at once.",
                     SystemSettings.maximumThreadCount() + 1);
        List<Future<List<IngestResult>>> savingSources = new ArrayList<>();

        // Create the sources for which ingest should be attempted, together with
        // any required links. A link is an additional entry in wres.ProjectSource.
        // A link is required for each context in which the source appears within
        // a project. A context means LeftOrRightOrBaseline.
        Set<DataSource> sources = SourceLoader.createSourcesToLoadAndLink( projectConfig );

        LOGGER.debug( "Created these sources to load and link: {}", sources );

        // Load each source and create any additional links required
        for( DataSource source : sources )
        {
            savingSources.addAll( this.loadSource( source ) );
        }

        return Collections.unmodifiableList( savingSources );
    }

    /**
     * Attempts to load the input source.
     * 
     * @param source The data source
     * @return A listing of asynchronous tasks dispatched to ingest data
     * @throws FileNotFoundException when a source file is not found
     * @throws IOException when a source file was not readable
     */
    private List<Future<List<IngestResult>>> loadSource( DataSource source )
            throws IOException
    {
        // Try to load non-file source
        Future<List<IngestResult>> nonFileIngest = loadNonFileSource( source );

        // When the non-file source is detected, short-circuit the file way.
        if ( nonFileIngest != null )
        {
            return Collections.singletonList( nonFileIngest );
        }

        // Proceed with files
        List<Future<List<IngestResult>>> savingFiles = new ArrayList<>();

        if ( !source.hasSourcePath() )
        {
            throw new FileNotFoundException( "Found a file data source with an invalid path: "
                                             + source );
        }

        File sourceFile = Paths.get( source.getUri() ).toFile();

        if ( !sourceFile.exists() )
        {
            throw new FileNotFoundException( "The path: '" +
                                             sourceFile.getCanonicalPath()
                                             +
                                             "' was not found." );
        }
        else if ( !sourceFile.canRead() )
        {
            throw new IOException( "The path: '" + sourceFile.getCanonicalPath()
                                   + "' was not readable. Please set "
                                   + "the permissions of that path to "
                                   + "readable for user '"
                                   + System.getProperty( "user.name" )
                                   + "' or run WRES as a user with read"
                                   + " permissions on that path." );
        }
        else if ( sourceFile.isFile() )
        {
            List<Future<List<IngestResult>>> futureResults =
                    SourceLoader.ingestFile( source,
                                             this.projectConfig,
                                             this.lockManager );
            savingFiles.addAll( futureResults );
        }
        else
        {
            LOGGER.warn( "'{}' is not a source of valid input data.",
                         sourceFile.getCanonicalPath() );
        }

        return Collections.unmodifiableList( savingFiles );
    }

    /**
     * Load a given source from a given config, return null if file-like source
     * 
     * TODO: create links for a non-file source when it appears in more than 
     * one context, i.e. {@link LeftOrRightOrBaseline}. 
     * See {@link #ingestData(DataSource, ProjectConfig, DatabaseLockManager)}
     * for how this is done with a file source.
     * 
     * @param source the data source
     * @return a single future list of results or null if source was file-like
     */

    private Future<List<IngestResult>> loadNonFileSource( DataSource source )
    {
        // See #63493. This method of identification, which is tied to 
        // source format, does not work well. The format should not designate
        // whether a source originates from a file or from a service. 
        // Also, there is an absence of consistency in whether a service-like
        // source requires that the URI is declared. For example, at the time
        // of writing, it is required for WRDS, but not for USGS NWIS.
        // As a result, expect some miss-identification of sources as 
        // originating from services vs. files.

        URI sourceUri = source.getSource()
                              .getValue();

        if ( sourceUri != null
             && sourceUri.getScheme() != null
             && sourceUri.getHost() != null )
        {
            WebSource webSource = WebSource.of( projectConfig,
                                                source,
                                                this.lockManager );
            return Executor.submit( webSource );
        }
        else if ( source.getSource().getFormat() == Format.USGS)
        {
            if (ConfigHelper.isForecast( source.getContext() ))
            {
                throw new IllegalArgumentException( "USGS data cannot be used to supply forecasts." );
            }

            // Should use the uri containing usgs.gov instead of faking a name.
            DataSource fakeDataSource = DataSource.of( source.getSource(),
                                                       source.getContext(),
                                                       source.getLinks(),
                                                       URI.create( "usgs" ) );
            return Executor.submit(
                    IngestSaver.createTask()
                               .withProject( this.projectConfig )
                               .withDataSource( fakeDataSource )
                               .withLockManager( this.lockManager )
                               .withoutHash()
                               .build()
            );
        }
        else if ( source.getSource().getFormat() == Format.S_3 )
        {
            // Should use the uri containing usgs.gov instead of faking a name
            DataSource fakeDataSource = DataSource.of( source.getSource(),
                                                       source.getContext(),
                                                       source.getLinks(),
                                                       URI.create( "s3" ) );
            return Executor.submit(
                    IngestSaver.createTask()
                               .withProject( this.projectConfig )
                               .withDataSource( fakeDataSource )
                               .withLockManager( this.lockManager )
                               .withoutHash()
                               .build()
            );
        }
        else
        {
            // At this point we should have a file, but check first.
            if ( sourceUri == null )
            {
                throw new ProjectConfigException( source.getSource(),
                                                  "Unable to use the source "
                                                  + "because no URI was "
                                                  + "specified." );
            }

            // Null signifies the source was a file-ish source.
            return null;
        }
    }


    /**
     * Ingest data where the hash is known in advance. This is one of the two
     * innermost versions of the ingestData method.
     * @param source the source to ingest
     * @param projectConfig the project configuration causing the ingest
     * @param lockManager the lock manager to use
     * @param hash the hash of the source data
     * @return a list of future lists of ingest results, possibly empty
     */

    private static List<Future<List<IngestResult>>> ingestData( DataSource source,
                                                                ProjectConfig projectConfig,
                                                                DatabaseLockManager lockManager,
                                                                String hash )
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( lockManager );

        if ( hash == null || hash.isBlank() )
        {
            throw new IllegalArgumentException( "This ingestData must be called "
                                                + "only when the hash is "
                                                + "already known, the hash "
                                                + "must be non-null and not "
                                                + "blank." );
        }

        List<Future<List<IngestResult>>> tasks = new ArrayList<>();

        IngestSaver ingestSaver = IngestSaver.createTask()
                                             .withProject( projectConfig )
                                             .withDataSource( source )
                                             .withHash( hash )
                                             .withProgressMonitoring()
                                             .withLockManager( lockManager )
                                             .build();

        Future<List<IngestResult>> task = Executor.submit( ingestSaver );
        tasks.add( task );

        // Additional links required?
        // If so, create a fake ingest result for each source type to link.
        // The context is provided by the corresponding data source declaration.

        // TODO (JBr): it smells that we do not mutate wres.Source and wres.ProjectSource
        // atomically. Instead, we delegate the mutation of wres.ProjectSource to
        // Operations, which we notify here with a fake Future. The SourceLoader needs
        // to become responsible for both.
        for ( LeftOrRightOrBaseline nextLink : source.getLinks() )
        {
            // Get the context for the link
            DataSourceConfig dataSourceconfig = SourceLoader.getDataSourceConfig( projectConfig, nextLink );
            DataSource anotherDataSource = source.withContext( dataSourceconfig );

            // Fake a future, return result immediately.
            tasks.add( IngestResult.fakeFutureSingleItemListFrom( projectConfig,
                                                                  anotherDataSource,
                                                                  hash ) );
        }

        return Collections.unmodifiableList( tasks );
    }


    /**
     * Ingest data where the the source is known to be a file, but the hash is
     * not yet known. This method uses the more generic ingestData method after
     * computing the hash.
     *
     * @param source the source to ingest, must be a file
     * @param projectConfig the project configuration causing the ingest
     * @param lockManager the lock manager to use
     * @return a list of future lists of ingest results, possibly empty
     */
    private static List<Future<List<IngestResult>>> ingestFile( DataSource source,
                                                                ProjectConfig projectConfig,
                                                                DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( lockManager );

        URI sourceUri = source.getUri();
        List<Future<List<IngestResult>>> tasks = new ArrayList<>();
        FileEvaluation checkIngest = shouldIngest( source.getUri(),
                                                   source.getSource(),
                                                   source.getContext().getVariable().getValue(),
                                                   lockManager );

        if ( checkIngest.shouldIngest() )
        {
            // When there is an archive, shouldIngest() will be true however
            // the hash will not yet have been computed, because the inner
            // source identities are what is important, and those inner sources
            // will be hashed later in the process.
            if ( checkIngest.getHash() == null )
            {
                IngestSaver ingestSaver = IngestSaver.createTask()
                                                     .withProject( projectConfig )
                                                     .withDataSource( source )
                                                     .withoutHash()
                                                     .withProgressMonitoring()
                                                     .withLockManager( lockManager )
                                                     .build();
                Future<List<IngestResult>> future = Executor.submit( ingestSaver );
                tasks.add( future );
            }
            else
            {
                List<Future<List<IngestResult>>> futureList =
                        SourceLoader.ingestData( source,
                                                 projectConfig,
                                                 lockManager,
                                                 checkIngest.getHash() );
                tasks.addAll( futureList );
            }
        }
        else
        {
            if ( checkIngest.isValid() )
            {
                // When the ingest requires retry and also is not in progress,
                // throw an exception: some process trying to ingest the source
                // died during ingest and data needs to be cleaned out.
                if ( !checkIngest.ingestMarkedComplete()
                     && !checkIngest.ingestInProgress() )
                {
                    throw new IllegalStateException( "Another WRES instance"
                                                     + " started to ingest "
                                                     + checkIngest.hash
                                                     + " but did not finish." );
                }

                LOGGER.debug( "Data will not be loaded from '{}'. That data is already in the database",
                              sourceUri );

                // Fake a future, return result immediately.
                tasks.add( IngestResult.fakeFutureSingleItemListFrom( projectConfig,
                                                                      source,
                                                                      checkIngest.getHash(),
                                                                      !checkIngest.ingestMarkedComplete() ) );

                // Additional links required? The source already exists, but the links may not
                for ( LeftOrRightOrBaseline nextLink : source.getLinks() )
                {
                    // Get the context for the link
                    DataSourceConfig dataSourceconfig = SourceLoader.getDataSourceConfig( projectConfig, nextLink );
                    DataSource anotherDataSource = source.withContext( dataSourceconfig );

                    // Fake a future, return result immediately.
                    tasks.add( IngestResult.fakeFutureSingleItemListFrom( projectConfig,
                                                                          anotherDataSource,
                                                                          checkIngest.getHash() ) );
                }
            }
            else
            {
                LOGGER.warn( "Data will not be loaded from invalid URI '{}'",
                             sourceUri );
            }
        }

        LOGGER.trace( "ingestData returning tasks {} for URI {}",
                      tasks,
                      sourceUri );

        return Collections.unmodifiableList( tasks );
    }


    /**
     * Helper that returns the data source context from a project for a specified type
     * of context.
     * 
     * @param projectConfig the project
     * @param type the the type of context
     * @return the context
     * @throws IllegalArgumentException if the type is unrecognized
     * @throws NullPointerException if either input is null
     */
    
    private static DataSourceConfig getDataSourceConfig( ProjectConfig projectConfig, LeftOrRightOrBaseline type )
    {
        Objects.requireNonNull( projectConfig );
        
        Objects.requireNonNull( type );
        
        switch ( type )
        {
            case LEFT:
                return projectConfig.getInputs().getLeft();
            case RIGHT:
                return projectConfig.getInputs().getRight();
            case BASELINE:
                return projectConfig.getInputs().getBaseline();
            default: throw new IllegalArgumentException( "Unrecognized type '"+type+"'." );
        }

    }
    
    /**
     * Determines whether or not data at an indicated path should be ingested.
     * archived data will always be further evaluated to determine whether its
     * individual entries warrent an ingest
     * @param filePath The path of the file to evaluate
     * @param source The configuration indicating that the given file might
     *               need to be ingested
     * @return Whether or not data within the file should be ingested (and hash)
     * @throws PreIngestException when hashing or id lookup cause some exception
     */
    private static FileEvaluation shouldIngest( final URI filePath,
                                                final DataSourceConfig.Source source,
                                                final String variableName,
                                                DatabaseLockManager lockManager )
    {
        Format specifiedFormat = source.getFormat();
        Format pathFormat = ReaderFactory.getFiletype( filePath );

        // Archives perform their own ingest verification
        if ( pathFormat == Format.ARCHIVE )
        {
            LOGGER.debug( "The data at '{}' will be marked as ingested because it has " +
                          "determined that it is an archive that will need to " +
                          "be further evaluated.",
                          filePath);
            return new FileEvaluation( true, true, null, false, false );
        }

        boolean ingest = specifiedFormat == null ||
                         specifiedFormat.equals( pathFormat );
        boolean anotherTaskStartedIngest = false;
        boolean ingestMarkedComplete = false;
        boolean ingestInProgress = false;

        String hash;

        if (ingest)
        {
            try
            {
                // If the format is Netcdf, we want to possibly bypass traditional hashing
                if (pathFormat == Format.NET_CDF)
                {
                    hash = NetCDF.getUniqueIdentifier(filePath, variableName);
                }
                else
                {
                    hash = Strings.getMD5Checksum( filePath );
                }

                anotherTaskStartedIngest = anotherTaskIsResponsibleForSource( hash );

                if ( anotherTaskStartedIngest )
                {
                    ingestMarkedComplete = anotherTaskReportsSourceCompleted( hash );

                    if ( !ingestMarkedComplete )
                    {
                        LOGGER.debug( "Another task is responsible for {} but has not yet finished it.", hash );
                        ingestInProgress = ingestInProgress( hash, lockManager );
                        LOGGER.debug( "Is another task currently ingesting {}? {}",
                                      hash, ingestInProgress );
                    }
                }

                // Added in debugging #58715-116
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Determined that a source with URI {} and "
                                  + "hash {} has the following status of "
                                  + "completely existing within the database: "
                                  + "{}, and status of another task claiming "
                                  + "responsibility: {}, and status of another "
                                  + "task currently ingesting: {}",
                                  filePath,
                                  hash,
                                  ingestMarkedComplete,
                                  anotherTaskStartedIngest,
                                  ingestInProgress );
                }
            }
            catch ( IOException | SQLException e )
            {
                throw new PreIngestException( "Could not determine whether to ingest '"
                                              +  filePath + "'", e );
            }
        }
        else
        {
            LOGGER.debug( "The file at '{}' will not be ingested because it " +
                          "does not match the specified required format. " +
                          "(specified: {}, encountered: {})",
                          filePath,
                          specifiedFormat,
                          pathFormat );
            return new FileEvaluation( false,
                                       false,
                                       null,
                                       false,
                                       false );
        }

        return new FileEvaluation( true, !anotherTaskStartedIngest, hash, ingestMarkedComplete, ingestInProgress );
    }

    /**
     * Determines if the indicated data is currently being ingested by a task
     * in another process.
     * @param hash The hash of the data that some task might be ingesting, known
     *             to exist already.
     * @return true if a task is detected to be ingesting, false otherwise
     * @throws SQLException When communication with the database fails.
     */
    private static boolean ingestInProgress( String hash,
                                             DatabaseLockManager lockManager )
            throws SQLException
    {
        SourceDetails sourceDetails = DataSources.getExistingSource( hash );
        Integer sourceId = sourceDetails.getId();
        return lockManager.isSourceLocked( sourceId );
    }

    /**
     * Determines if the indicated data already exists within the database
     * @param hash The hash of the file that might need to be ingested
     * @return Whether or not another task has claimed responsibility for data
     * @throws SQLException Thrown if communcation with the database failed in
     * some way
     */
    private static boolean anotherTaskIsResponsibleForSource( String hash )
            throws SQLException
    {
        return DataSources.hasSource( hash );
    }


    /**
     * Returns true when another task has reported the data ingest complete,
     * false otherwise.
     * @param hash the data to look for
     * @return Whether the data has been completely ingested.
     * @throws SQLException when query fails
     * @throws NullPointerException when the caller failed to verify that a
     * task already claimed the hash passed in by calling
     * anotherTaskIsResponsibleForSource()
     */
    private static boolean anotherTaskReportsSourceCompleted( String hash )
            throws SQLException
    {
        SourceDetails details = DataSources.getExistingSource( hash );
        SourceCompletedDetails completedDetails = new SourceCompletedDetails( details );
        return completedDetails.wasCompleted();
    }

    public List<Future<List<IngestResult>>> retry( IngestResult ingestResult )
    {
        if ( !ingestResult.requiresRetry() )
        {
            throw new IllegalArgumentException( "Only IngestResult instances claiming to need retry should be passed." );
        }

        LOGGER.info( "Attempting retry of {}.", ingestResult );

        return SourceLoader.ingestData( ingestResult.getDataSource(),
                                        this.projectConfig,
                                        this.lockManager,
                                        ingestResult.getHash() );
    }

    /**
     * A result of file evaluation containing whether the file was valid,
     * whether the file should be ingested, and the hash if available.
     */
    private static class FileEvaluation
    {
        private final boolean isValid;
        private final boolean shouldIngest;
        private final String hash;
        private final boolean ingestMarkedComplete;
        private final boolean ingestInProgress;

        FileEvaluation( boolean isValid,
                        boolean shouldIngest,
                        String hash,
                        boolean ingestMarkedComplete,
                        boolean ingestInProgress )
        {
            this.isValid = isValid;
            this.shouldIngest = shouldIngest;
            this.hash = hash;
            this.ingestMarkedComplete = ingestMarkedComplete;
            this.ingestInProgress = ingestInProgress;
        }

        public boolean isValid()
        {
            return this.isValid;
        }

        boolean shouldIngest()
        {
            return this.shouldIngest;
        }

        public String getHash()
        {
            return this.hash;
        }

        public boolean ingestMarkedComplete()
        {
            return this.ingestMarkedComplete;
        }

        public boolean ingestInProgress()
        {
            return this.ingestInProgress;
        }
    }

    /**
     * <p>Evaluates a project and creates a {@link DataSource} for each 
     * distinct source within the project that needs to
     * be loaded, together with any additional links required. A link is required 
     * for each additional context, i.e. {@link LeftOrRightOrBaseline}, in 
     * which the source appears. The links are returned by {@link DataSource#getLinks()}.
     * Here, a "link" means a separate entry in <code>wres.ProjectSource</code>.
     * 
     * <p>A {@link DataSource} is returned for each discrete source. When the declared
     * {@link DataSourceConfig.Source} points to a directory of files, the tree 
     * is walked and a {@link DataSource} is returned for each one within the tree 
     * that meets any prescribed filters.
     * 
     * @return the set of distinct sources to load and any additional links to create
     * @throws NullPointerException if the input is null
     */

    private static Set<DataSource> createSourcesToLoadAndLink( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        // Somewhat convoluted structure that will be turned into a simple one.
        // The key is the distinct source, and the paired value is the context in
        // which the source appears and the set of additional links to create, if any.
        // Note that all project declaration overrides hashCode and equals (~ key in a HashMap)
        Map<DataSourceConfig.Source, Pair<DataSourceConfig, Set<LeftOrRightOrBaseline>>> sources = new HashMap<>();

        // Must have one or more left sources to load and link
        SourceLoader.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getLeft() );

        // Must have one or more right sources to load and link
        SourceLoader.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getRight() );

        // May have one or more baseline sources to load and link
        if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
        {
            SourceLoader.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getBaseline() );
        }

        // Create a simple entry (DataSource) for each complex entry
        Set<DataSource> returnMe = new HashSet<>();

        // Expand any file sources that represent directories and filter any that are not required
        for ( Map.Entry<DataSourceConfig.Source, Pair<DataSourceConfig, Set<LeftOrRightOrBaseline>>> nextSource : sources.entrySet() )
        {
            // Evaluate the path, which is null for a source that is not file-like
            Path path = SourceLoader.evaluatePath( nextSource.getKey() );

            // If there is a file-like source, test for a directory and decompose it as required
            if( Objects.nonNull( path ) )
            {
                DataSource source = DataSource.of( nextSource.getKey(),
                                                   nextSource.getValue()
                                                             .getLeft(),
                                                   nextSource.getValue()
                                                             .getRight(),
                                                   path.toUri() );

                returnMe.addAll( SourceLoader.decomposeFileSource( source ) );
            }
            // Not a file-like source
            else
            {
                DataSource source = DataSource.of( nextSource.getKey(),
                                                   nextSource.getValue()
                                                             .getLeft(),
                                                   nextSource.getValue()
                                                             .getRight(),
                                                   nextSource.getKey()
                                                             .getValue() );
                returnMe.add( source );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Helper that decomposes a file-like source into other sources. In particular, 
     * if the declared source represents a directory, walk the tree and find sources 
     * that match any prescribed pattern. Return a {@link DataSource}
     * 
     * @param dataSource the source to decompose
     * @return the set of decomposed sources
     */
    
    private static Set<DataSource> decomposeFileSource( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        
        // Look at the path to see whether it maps to a directory
        Path sourcePath = Paths.get( dataSource.getUri() );

        File file = sourcePath.toFile();
        
        Set<DataSource> returnMe = new HashSet<>();
        
        // Directory: must decompose into sources
        if( file.isDirectory() )
        {

            DataSourceConfig.Source source = dataSource.getSource();
            
            //Define path matcher based on the source's pattern, if provided.
            final PathMatcher matcher;
            
            String pattern = source.getPattern();
                    
            if ( !com.google.common.base.Strings.isNullOrEmpty( pattern ) )
            {
                matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern );
            }
            else
            {
                matcher = null;
            }
            
            // Walk the tree and find sources that match a pattern or none
            try ( Stream<Path> files = Files.walk( sourcePath ) )
            {            
                files.forEach( path -> {

                    File testFile = path.toFile();

                    //File must be a file and match the pattern, if the pattern is defined.
                    if ( testFile.isFile() && ( ( matcher == null ) || matcher.matches( path ) ) )
                    {
                        returnMe.add( DataSource.of( dataSource.getSource(),
                                                     dataSource.getContext(),
                                                     dataSource.getLinks(),
                                                     path.toUri() ) );
                    }
                    // Skip and log a warning if this is a normal file (e.g. not a directory) 
                    else if ( testFile.isFile() )
                    {
                        LOGGER.warn( "Skipping {} because it does not match pattern \"{}\".",
                                     path,
                                     pattern );
                    }
                } );

            }
            catch ( IOException e )
            {
                throw new PreIngestException( "Failed to walk the directory tree '"
                                              + sourcePath + "':", e );
            }

            //If the results are empty, then there were either no files in the specified source or pattern matched 
            //none of the files.  
            if ( returnMe.isEmpty() )
            {
                throw new PreIngestException( "The pattern of \"" + pattern
                                              + "\" does not yield any files within the provided "
                                              + "source path and is therefore not a valid source." );
            }
            
        }
        else
        {
            returnMe.add( dataSource );
        }
        
        return Collections.unmodifiableSet( returnMe );
        
    }
    
    /**
     * Evaluate a path from a {@link DataSourceConfig.Source}.
     * @param source the source
     * @return the path of a file-like source or null
     */
    
    private static Path evaluatePath( DataSourceConfig.Source source )
    {       
        // Is there a source path to evaluate? Only if the source is file-like
        if( source.getValue().toString().isEmpty() )
        {
            return null;           
        }
        
        Path rawSourcePath = Paths.get( source.getValue().getPath() );
        
        LOGGER.debug( "Found source path {} from source {}",
                      rawSourcePath,
                      source.getValue() );

        // In the straightforward case, use the source path found.
        Path sourcePath = rawSourcePath;

        // Construct a path using the SystemSetting wres.dataDirectory when
        // the specified source is not absolute.
        if ( !rawSourcePath.isAbsolute() )
        {
            sourcePath = SystemSettings.getDataDirectory()
                                       .resolve( rawSourcePath );
        }
        
        return sourcePath;
        
    }
 
    /**
     * Mutates the input map of sources, adding additional sources to load or link
     * from the input {@link DataSourceConfig}.
     * 
     * @param sources the map of sources to mutate
     * @param projectConfig the project configuration
     * @param dataSourceConfig the data source configuration for which sources to load or link are required
     * @throws NullPointerException if any input is null
     */

    private static void
            mutateSourcesToLoadAndLink( Map<DataSourceConfig.Source, Pair<DataSourceConfig, Set<LeftOrRightOrBaseline>>> sources,
                                        ProjectConfig projectConfig,
                                        DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( sources );

        Objects.requireNonNull( projectConfig );

        Objects.requireNonNull( dataSourceConfig );

        LeftOrRightOrBaseline sourceType = ConfigHelper.getLeftOrRightOrBaseline( projectConfig, dataSourceConfig );

        // Must have one or more right sources
        for ( DataSourceConfig.Source source : dataSourceConfig.getSource() )
        {
            // Link or load?
            // NOTE: there are some paired contexts in which it would be wrong for
            // the source to appear together (e.g. both left and right),
            // but that validation needs to happen way before now, so proceed in all cases

            // Only create a link if the source is already in the load list/map
            if ( sources.containsKey( source ) )
            {
                // Only link sources that appear in a different context
                if ( ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                            sources.get( source )
                                                                   .getLeft() ) != sourceType )
                {
                    sources.get( source ).getRight().add( sourceType );
                }
            }
            // Load
            else
            {
                sources.put( source, Pair.of( dataSourceConfig, new HashSet<>() ) );
            }
        }

    }
}
