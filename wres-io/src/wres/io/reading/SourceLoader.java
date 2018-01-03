package wres.io.reading;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import wres.io.concurrency.ObservationSaver;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.util.Internal;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * Evaluates datasources specified within a project configuration and determines
 * what data should be ingested. Asynchronous tasks for each file needed for
 * ingest are created and sent to the Exector for ingestion.
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class SourceLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);

    /**
     * @param projectConfig the project configuration
     */
    @Internal(exclusivePackage = "wres.io")
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
        List<Future<List<IngestResult>>> savingFiles = new ArrayList<>();

        savingFiles.addAll(loadConfig(getLeftSource(), LeftOrRightOrBaseline.LEFT));
        savingFiles.addAll(loadConfig(getRightSource(), LeftOrRightOrBaseline.RIGHT ));
        savingFiles.addAll(loadConfig(getBaseLine(), LeftOrRightOrBaseline.BASELINE ));

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
    private List<Future<List<IngestResult>>> loadConfig( DataSourceConfig config,
                                                         LeftOrRightOrBaseline leftOrRightOrBaseline )
            throws IOException
    {
        List<Future<List<IngestResult>>> savingFiles = new ArrayList<>();

        ProgressMonitor.increment();

        if (config != null) {
            for (DataSourceConfig.Source source : config.getSource())
            {
                if ( source.getFormat() == Format.USGS)
                {
                    if (ConfigHelper.isForecast( config ))
                    {
                        throw new IllegalArgumentException( "USGS data cannot be used to supply forecasts." );
                    }

                    savingFiles.add( Executor.submit( new ObservationSaver( "usgs",
                                                                            this.projectConfig,
                                                                            config,
                                                                            source,
                                                                            this.projectConfig
                                                                                    .getPair()
                                                                                    .getFeature() ) ));
                    continue;
                }

                Path sourcePath = Paths.get(source.getValue());

                if (!Files.exists( sourcePath ))
                {
                    throw new FileNotFoundException( "The path: '" +
                                                     sourcePath +
                                                     "' was not found.");
                }
                else if ( !Files.isReadable( sourcePath ) )
                {
                    throw new IOException( "The path: '" + sourcePath
                                           + "' was not readable. Please set "
                                           + "the permissions of that path to "
                                           + "readable for user '"
                                           + System.getProperty( "user.name" )
                                           + "' or run WRES as a user with read"
                                           + " permissions on that path." );
                }
                else if (Files.isDirectory(sourcePath)) {

                    List<Future<List<IngestResult>>> tasks =
                            loadDirectory( sourcePath,
                                           source,
                                           config );

                    savingFiles.addAll( tasks );
                }
                else if ( Files.isRegularFile( sourcePath ) )
                {
                    // TODO: It might be useful to go ahead and just create the
                    // archive ingester here instead of trying to jump through
                    // the series of validations

                    Future<List<IngestResult>> task = saveFile( sourcePath,
                                                                source,
                                                                config );

                    if (task != null)
                    {
                        savingFiles.add(task);
                    }
                }
                else
                {
                    LOGGER.error("'{}' is not a source of valid input data.", source.getValue());
                }
            }
        }

        ProgressMonitor.completeStep();

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
     */
    private List<Future<List<IngestResult>>> loadDirectory( Path directory,
                                                            DataSourceConfig.Source source,
                                                            DataSourceConfig dataSourceConfig )
    {
        List<Future<List<IngestResult>>> results = new ArrayList<>();
        Stream<Path> files;

        ProgressMonitor.increment();

        try
        {
            files = Files.walk(directory);

            files.forEach((Path path) -> {
                if (Files.notExists(path))
                {
                    throw new IllegalArgumentException( "The source path of" +
                                                        path.toAbsolutePath().toString() +
                                                        " does not exist and is therefore not a valid source.");
                }

                if (directory.equals( path ))
                {
                    return;
                }

                if (Files.isRegularFile(path))
                {
                    Future<List<IngestResult>> task = saveFile( path,
                                                                source,
                                                                dataSourceConfig );

                    if (task != null)
                    {
                        results.add(task);
                    }
                }
            });

            files.close();
        }
        catch (IOException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }

        ProgressMonitor.completeStep();

        return Collections.unmodifiableList( results );
    }



    /**
     * saveFile returns Future on success, null in several cases.
     * Caller must expect null and handle it appropriately.
     *
     * @param filePath
     * @return Future if task was created, null otherwise.
     */
    private Future<List<IngestResult>> saveFile( Path filePath,
                                                 DataSourceConfig.Source source,
                                                 DataSourceConfig dataSourceConfig )
    {
        String absolutePath = filePath.toAbsolutePath().toString();
        Future<List<IngestResult>> task = null;

        ProgressMonitor.increment();

        FileEvaluation checkIngest = shouldIngest( absolutePath,
                                                   source,
                                                   dataSourceConfig );

        if ( checkIngest.shouldIngest() )
        {
            try
            {
                if (ConfigHelper.isForecast(dataSourceConfig))
                {
                    LOGGER.trace("Loading {} as forecast data...", absolutePath);
                    task = Executor.submit( new ForecastSaver( absolutePath,
                                                               this.projectConfig,
                                                              dataSourceConfig,
                                                              source,
                                                              this.getSpecifiedFeatures() ) );
                }
                else
                {
                    LOGGER.trace("Loading {} as Observation data...");
                    task = Executor.submit( new ObservationSaver( absolutePath,
                                                                  this.projectConfig,
                                                                 dataSourceConfig,
                                                                 source,
                                                                 this.getSpecifiedFeatures() ));
                }
            }
            catch (SQLException sqlException)
            {
                LOGGER.error("'" + absolutePath + "' could not be queued for ingest.");
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

        ProgressMonitor.completeStep();

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
     * @param dataSourceConfig The overall configuration indicating that the
     *                         file should be evaluated for ingestion
     * @return Whether or not data within the file should be ingested (and hash)
     */
    private FileEvaluation shouldIngest( String filePath,
                                         DataSourceConfig.Source source,
                                         DataSourceConfig dataSourceConfig )
    {
        SourceType specifiedFormat = ReaderFactory.getFileType(source.getFormat());
        SourceType pathFormat = ReaderFactory.getFiletype(filePath);

        // Archives perform their own ingest verification
        if (pathFormat == SourceType.ARCHIVE)
        {
            LOGGER.debug( "The file at '{}' will be ingested because it has " +
                          "determined that it is an archive that will need to " +
                          "be further evaluated.",
                          filePath);
            return new FileEvaluation( true, true, null );
        }

        boolean ingest = specifiedFormat == SourceType.UNDEFINED ||
                         specifiedFormat.equals(pathFormat);

        String hash = null;

        if (ingest)
        {
            try
            {
                hash = Strings.getMD5Checksum( filePath );
                ingest = !dataExists( hash, dataSourceConfig );
            }
            catch ( SQLException e )
            {
                LOGGER.warn( "Could not determine whether to ingest {}",
                             filePath, e );
                ingest = false;
            }
            catch ( IOException ioe )
            {
                throw new RuntimeException( "Problem reading file {}", ioe );
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
                          specifiedFormat.toString(),
                          pathFormat.toString());
            return new FileEvaluation( false, false, null );
        }

        return new FileEvaluation( true, ingest, hash );
    }

    /**
     * Determines if the indicated data already exists within the database
     * @param hash The hash of the file that might need to be ingested
     * @param dataSourceConfig The configuration for the set of data sources that
     *                         indicated that this file might need to be ingested
     * @return Whether or not the indicated data is already in the database
     * @throws SQLException Thrown if communcation with the database failed in
     * some way
     */
    private boolean dataExists(String hash, DataSourceConfig dataSourceConfig)
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
            Set<Feature> atomicFeatures = new HashSet<>();

            for ( FeatureDetails details : Features.getAllDetails( projectConfig ) )
            {
                atomicFeatures.add( details.toFeature() );
            }
            this.specifiedFeatures = new ArrayList<>(atomicFeatures);
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
