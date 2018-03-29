package wres.io.reading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.IngestSaver;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.util.ProgressMonitor;
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
     * @param projectConfig the project configuration
     */
    public SourceLoader (ProjectConfig projectConfig)
    {
        this.projectConfig = projectConfig;
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
        List<Future<List<IngestResult>>> savingFiles = new ArrayList<>();

        savingFiles.addAll( loadConfig( getLeftSource() ) );
        savingFiles.addAll( loadConfig( getRightSource() ) );
        savingFiles.addAll( loadConfig( getBaseLine() ) );

        return savingFiles;
    }

    /**
     * Searches through the specifications for a set of datasources and
     * determines what needs to be ingested
     * @param config The configuration for a set of datasources
     * @return A listing of asynchronous tasks dispatched to ingest data
     * @throws FileNotFoundException when a source file is not found
     * @throws IOException when a source file was not readable
     */
    private List<Future<List<IngestResult>>> loadConfig( DataSourceConfig config )
            throws IOException
    {
        List<Future<List<IngestResult>>> savingFiles = new ArrayList<>();

        //ProgressMonitor.increment();

        if (config != null) {
            for (DataSourceConfig.Source source : config.getSource())
            {
                if ( source.getFormat() == Format.USGS)
                {
                    if (ConfigHelper.isForecast( config ))
                    {
                        throw new IllegalArgumentException( "USGS data cannot be used to supply forecasts." );
                    }

                    savingFiles.add( Executor.submit( new IngestSaver( "usgs",
                                                                            this.projectConfig,
                                                                            config,
                                                                            source,
                                                                            this.projectConfig
                                                                                    .getPair()
                                                                                    .getFeature() ) ));
                    continue;
                }

                Path sourcePath = Paths.get(source.getValue());
                File sourceFile = sourcePath.toFile();

                if ( !sourceFile.exists() )
                {
                    throw new FileNotFoundException( "The path: '" +
                                                     sourceFile.getCanonicalPath() +
                                                     "' was not found.");
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
                else if ( sourceFile.isDirectory() )
                {
                    List<Future<List<IngestResult>>> tasks =
                            loadDirectory( sourcePath,
                                           source,
                                           config );

                    savingFiles.addAll( tasks );
                }
                else if ( sourceFile.isFile() )
                {
                    List<Feature> specifiedFeatures;

                    try
                    {
                        specifiedFeatures = this.getSpecifiedFeatures();
                    }
                    catch ( SQLException se )
                    {
                        throw new IOException( "Could not get specified features.", se );
                    }

                    Future<List<IngestResult>> task = saveFile( sourcePath,
                                                                source,
                                                                config,
                                                                specifiedFeatures,
                                                                this.projectConfig );

                    if (task != null)
                    {
                        savingFiles.add(task);
                    }
                }
                else if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( "'{}' is not a source of valid input data.",
                                 sourceFile.getCanonicalPath() );
                }
            }
        }

        //ProgressMonitor.completeStep();

        return Collections.unmodifiableList( savingFiles );
    }

    /**
     * Looks through a directory to find data that is needed for ingest
     * @param directory The path of the directory to evaluate
     * @param source The source configuration that indicated that the directory
     *               needed to be evaluated
     * @param dataSourceConfig The configuration of the set of data sources
     *                         that indicated that the directory needed to be
     *                         evaluated
     * @return A listing of asynchronous tasks dispatched to ingest data
     * @throws PreIngestException when walking the directory causes IOException
     */
    private List<Future<List<IngestResult>>> loadDirectory( Path directory,
                                                            DataSourceConfig.Source source,
                                                            DataSourceConfig dataSourceConfig )
    {
        List<Future<List<IngestResult>>> results = new ArrayList<>();

        //Define path matcher based on the source's pattern, if provided.
        final PathMatcher matcher;
        if (!com.google.common.base.Strings.isNullOrEmpty( source.getPattern() ))
        {
            matcher = FileSystems.getDefault().getPathMatcher("glob:" + source.getPattern());
        }
        else
        {
            matcher = null;
        }

        //ProgressMonitor.increment();

        List<Feature> specifiedFeatureses;

        try
        {
            specifiedFeatureses = this.getSpecifiedFeatures();
        }
        catch ( SQLException se )
        {
            throw new PreIngestException( "Failed to get list of features.", se );
        }

        Function<Path,Future<List<IngestResult>>> fileSaver =
                new FileSaver( matcher,
                               source,
                               dataSourceConfig,
                               specifiedFeatureses,
                               this.projectConfig );

        try ( Stream<Path> files = Files.walk( directory ) )
        {
            results.addAll( files.map( fileSaver )
                                 .filter( Objects::nonNull )
                                 .collect( Collectors.toList() ) );
        }
        catch ( IOException e )
        {
            throw new PreIngestException( "Failed to walk the directory tree '"
                                          + directory + "':", e );
        }

        //If the results are empty, then there were either no files in the specified source or pattern matched 
        //none of the files.  
        if (results.isEmpty())
        {
            throw new PreIngestException( "The pattern of \"" + source.getPattern()
                                          + "\" does not yield any files within the provided source path and is therefore not a valid source.");
        }
        
        //ProgressMonitor.completeStep();

        return Collections.unmodifiableList( results );
    }

    private static class FileSaver implements Function<Path,Future<List<IngestResult>>>
    {
        private final PathMatcher matcher;
        private final DataSourceConfig.Source source;
        private final DataSourceConfig dataSourceConfig;
        private final List<Feature> specifiedFeatures;
        private final ProjectConfig projectConfig;

        FileSaver( PathMatcher pathMatcher,
                   DataSourceConfig.Source source,
                   DataSourceConfig dataSourceConfig,
                   List<Feature> specifiedFeatures,
                   ProjectConfig projectConfig )
        {
            this.matcher = pathMatcher;
            this.source = source;
            this.dataSourceConfig = dataSourceConfig;
            this.specifiedFeatures = specifiedFeatures;
            this.projectConfig = projectConfig;
        }

        @Override
        public Future<List<IngestResult>> apply( Path path )
        {
            File file = path.toFile();

            if ( file.isDirectory() )
            {
                return null;
            }

            //File must be a file and match the pattern, if the pattern is defined.
            if ( file.isFile() && ((matcher == null) || matcher.matches( file.toPath())))
            {
                return SourceLoader.saveFile( path,
                                              this.source,
                                              this.dataSourceConfig,
                                              this.specifiedFeatures,
                                              this.projectConfig );
            }
            else
            {
                LOGGER.warn( "Skipping file {} because it does not match {}.",
                             file, matcher );
                return null;
            }
        }
    }

    /**
     * saveFile returns Future on success, null in several cases.
     * Caller must expect null and handle it appropriately.
     *
     * @param filePath
     * @return Future if task was created, null otherwise.
     * @throws SQLException if getting specified features fails
     */

    private static Future<List<IngestResult>> saveFile( Path filePath,
                                                        DataSourceConfig.Source source,
                                                        DataSourceConfig dataSourceConfig,
                                                        List<Feature> specifiedFeatures,
                                                        ProjectConfig projectConfig )
    {
        String absolutePath = filePath.toAbsolutePath().toString();
        Future<List<IngestResult>> task = null;

        //ProgressMonitor.increment();

        FileEvaluation checkIngest = shouldIngest( absolutePath,
                                                   source );

        if ( checkIngest.shouldIngest() )
        {
            if (ConfigHelper.isForecast(dataSourceConfig))
            {
                LOGGER.trace("Loading {} as forecast data...", absolutePath);
                task = Executor.submit( new IngestSaver( absolutePath,
                                                         projectConfig,
                                                         dataSourceConfig,
                                                         source,
                                                         specifiedFeatures ) );
            }
            else
            {
                LOGGER.trace("Loading {} as Observation data...");
                task = Executor.submit( new IngestSaver( absolutePath,
                                                         projectConfig,
                                                         dataSourceConfig,
                                                         source,
                                                         specifiedFeatures ));
            }
        }
        else
        {
            if ( checkIngest.isValid() )
            {
                LOGGER.debug(
                        "Data will not be loaded from '{}'. That data is already in the database",
                        absolutePath );

                // Fake a future, return result immediately.
                task = IngestResult.fakeFutureSingleItemListFrom( projectConfig,
                                                                  dataSourceConfig,
                                                                  checkIngest.getHash() );
            }
            else
            {
                LOGGER.warn( "Data will not be loaded from invalid file '{}'",
                             absolutePath );
                task = null;
            }
        }

        //ProgressMonitor.completeStep();

        LOGGER.trace( "saveFile returning task {} for filePath {}",
                      task,
                      filePath);

        return task;
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
    private static FileEvaluation shouldIngest( String filePath,
                                                DataSourceConfig.Source source )
    {
        Format specifiedFormat = source.getFormat();
        Format pathFormat = ReaderFactory.getFiletype( filePath );

        // Archives perform their own ingest verification
        if ( pathFormat == Format.ARCHIVE )
        {
            LOGGER.debug( "The file at '{}' will be ingested because it has " +
                          "determined that it is an archive that will need to " +
                          "be further evaluated.",
                          filePath);
            return new FileEvaluation( true, true, null );
        }

        boolean ingest = specifiedFormat == null ||
                         specifiedFormat.equals( pathFormat );

        String hash;

        if (ingest)
        {
            try
            {
                hash = Strings.getMD5Checksum( filePath );
                ingest = !dataExists( hash );
            }
            catch ( IOException | SQLException e )
            {
                throw new PreIngestException( "Could not determine whether to ingest '"
                                              +  filePath + "'", e );
            }

            if (!ingest)
            {
                LOGGER.debug( "The file at '{}' will not be ingested because " +
                              "the data is already registered as being in " +
                              "the system.",
                              filePath);
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
            return new FileEvaluation( false, false, null );
        }

        return new FileEvaluation( true, ingest, hash );
    }

    /**
     * Determines if the indicated data already exists within the database
     * @param hash The hash of the file that might need to be ingested
     * @return Whether or not the indicated data is already in the database
     * @throws SQLException Thrown if communcation with the database failed in
     * some way
     */
    private static boolean dataExists( String hash )
            throws SQLException
    {
        return DataSources.hasSource( hash );
    }

    /**
     * @return The data source configuration for the data on the left side of
     * the evaluation
     */
    private DataSourceConfig getLeftSource()
    {
        DataSourceConfig sourceConfig = null;

        if (projectConfig.getInputs().getLeft() != null)
        {
            sourceConfig = projectConfig.getInputs().getLeft();
        }

        return sourceConfig;
    }

    /**
     * @return The data source configuration for the data on the right side of
     * the evaluation
     */
    private DataSourceConfig getRightSource()
    {
        DataSourceConfig sourceConfig = null;

        if (projectConfig.getInputs().getRight() != null)
        {
            sourceConfig = projectConfig.getInputs().getRight();
        }

        return sourceConfig;
    }

    /**
     * @return The data source configuration for the data used as a baseline
     * for the evaluation
     */
    private DataSourceConfig getBaseLine()
    {
        DataSourceConfig source = null;

        if (projectConfig.getInputs().getBaseline() != null)
        {
            source = projectConfig.getInputs().getBaseline();
        }

        return source;
    }

    private List<Feature> getSpecifiedFeatures() throws SQLException
    {
        if (this.specifiedFeatures == null)
        {
            List<Feature> atomicFeatures = new ArrayList<>();

            for ( FeatureDetails details : Features.getAllDetails( projectConfig ) )
            {
                atomicFeatures.add( details.toFeature() );
            }

            this.specifiedFeatures = Collections.unmodifiableList( atomicFeatures );
        }
        return this.specifiedFeatures;
    }

    private List<Feature> specifiedFeatures;

    /**
     * The project configuration indicating what data to used
     */
    private final ProjectConfig projectConfig;


    /**
     * A result of file evaluation containing whether the file was valid,
     * whether the file should be ingested, and the hash if available.
     */
    private static class FileEvaluation
    {
        private final boolean isValid;
        private final boolean shouldIngest;
        private final String hash;

        FileEvaluation( boolean isValid,
                        boolean shouldIngest,
                        String hash )
        {
            this.isValid = isValid;
            this.shouldIngest = shouldIngest;
            this.hash = hash;
        }

        public boolean isValid()
        {
            return this.isValid;
        }

        public boolean shouldIngest()
        {
            return this.shouldIngest;
        }

        public String getHash()
        {
            return this.hash;
        }
    }
}
