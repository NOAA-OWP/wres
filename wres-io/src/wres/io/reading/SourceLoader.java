package wres.io.reading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import wres.io.concurrency.ObservationSaver;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Projects;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public class SourceLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);

    private static final String NEWLINE = System.lineSeparator();

    /**
     * @param projectConfig the project configuration
     */
    @Internal(exclusivePackage = "wres.io")
    public SourceLoader (ProjectConfig projectConfig) {
        this.projectConfig = projectConfig;
    }

    /**
     * Ingest data
     * @return List of Future file ingest results
     */
    public List<Future> load()
    {
        try
        {
            this.projectDetails = Projects.getProject( this.projectConfig );
        }
        catch ( SQLException e )
        {
            e.printStackTrace();
        }

        List<Future> savingFiles = new ArrayList<>();

        savingFiles.addAll(loadConfig(getLeftSource()));
        savingFiles.addAll(loadConfig(getRightSource()));
        savingFiles.addAll(loadConfig(getBaseLine()));

        return savingFiles;
    }

    private List<Future> loadConfig(DataSourceConfig config)
    {
        List<Future> savingFiles = new ArrayList<>();

        if (config != null) {
            for (DataSourceConfig.Source source : config.getSource()) {
                Path sourcePath = Paths.get(source.getValue());

                if (!Files.exists( sourcePath ))
                {
                    throw new IllegalStateException( "The path: '" +
                                                     source.getValue() +
                                                     "' is not a valid source of data.");
                }
                else if (Files.isDirectory(sourcePath)) {

                    List<Future> tasks = loadDirectory(sourcePath, source, config);

                    if (tasks != null)
                    {
                        savingFiles.addAll(tasks);
                    }
                }
                else if (Files.isRegularFile(sourcePath)) {
                    Future task = saveFile(sourcePath, source, config);

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

        return savingFiles;
    }

    private List<Future> loadDirectory(Path directory, DataSourceConfig.Source source, DataSourceConfig dataSourceConfig)
    {
        List<Future> results = new ArrayList<>();
        Stream<Path> files;

        try
        {
            files = Files.list(directory);

            files.forEach((Path path) -> {
                if (Files.notExists(path))
                {
                    throw new IllegalArgumentException( "The source path of" +
                                                        path.toAbsolutePath().toString() +
                                                        " does not exist and is therefore not a valid source.");
                }

                if (Files.isDirectory(path) && source.isRecursive())
                {
                    List<Future> tasks = loadDirectory(path, source, dataSourceConfig);

                    if (tasks != null)
                    {
                        results.addAll(tasks);
                    }
                }
                else if (Files.isRegularFile(path))
                {
                    Future task = saveFile(path, source, dataSourceConfig);

                    if (task != null)
                    {
                        results.add(task);
                    }
                }
            });

            files.close();
        }
        catch (IOException e) {
            LOGGER.error(Strings.getStackTrace(e));
        }

        return results;
    }

    /**
     * saveFile returns Future on success, null in several cases.
     * Caller must expect null and handle it appropriately.
     *
     * @param filePath
     * @return Future if task was created, null otherwise.
     */
    private Future saveFile(Path filePath, DataSourceConfig.Source source, DataSourceConfig dataSourceConfig)
    {
        String absolutePath = filePath.toAbsolutePath().toString();
        Future task = null;

        if (shouldIngest(absolutePath, source, dataSourceConfig))
        {
            if (!alreadySuspendedIndexes)
            {
                Database.suspendAllIndices();
                alreadySuspendedIndexes = true;
            }

            if (ConfigHelper.isForecast(dataSourceConfig)) {
                LOGGER.trace("Loading {} as forecast data...", absolutePath);
                task = Executor.execute(new ForecastSaver(absolutePath,
                                                          this.projectDetails,
                                                          dataSourceConfig,
                                                          dataSourceConfig.getFeatures()));
            }
            else {
                LOGGER.trace("Loading {} as Observation data...");
                task = Executor.execute(new ObservationSaver(absolutePath,
                                                             this.projectDetails,
                                                             dataSourceConfig,
                                                             dataSourceConfig.getFeatures()));
            }
        }
        else
        {
            String message = "Data will not be loaded from {}. That data is either not valid input data or is ";
            message += "already in the database.";
            LOGGER.debug(message, absolutePath);
        }

        return task;
    }

    private boolean shouldIngest(String filePath, DataSourceConfig.Source source, DataSourceConfig dataSourceConfig)
    {
        if (this.projectDetails.isEmpty())
        {
            return true;
        }

        SourceType specifiedFormat = ReaderFactory.getFileType(source.getFormat());
        SourceType pathFormat = ReaderFactory.getFiletype(filePath);

        // Archives perform their own ingest verification
        if (pathFormat == SourceType.ARCHIVE)
        {
            LOGGER.debug( "The file at '{}' will be ingested because it has " +
                          "determined that it is an archive that will need to " +
                          "be further evaluated.",
                          filePath);
            return true;
        }

        boolean ingest = specifiedFormat == SourceType.UNDEFINED ||
                         specifiedFormat.equals(pathFormat);

        if (ingest)
        {
            try {
                ingest = !dataExists(filePath, dataSourceConfig);
            }
            catch (SQLException e) {
                LOGGER.error(Strings.getStackTrace(e));
                ingest = false;
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
        }

        return ingest;
    }

    private boolean dataExists(String sourceName, DataSourceConfig dataSourceConfig) throws SQLException
    {
        boolean exists;

        try
        {

            String hash = Strings.getMD5Checksum( sourceName );

            // Check to see if the file has already been ingested for this project
            exists = this.projectDetails.hasSource( hash, dataSourceConfig);

            if (!exists)
            {
                // Check to see if some other project has ingested this data
                exists = DataSources.hasSource(hash);

                // If the data was ingested by another project...
                if (exists)
                {
                    // Add that source to this project
                    this.projectDetails.addSource( hash, dataSourceConfig);
                }
            }
        }
        catch ( IOException e )
        {
            LOGGER.error("The filesystem is reporting that the found source doesn't exist.");
            throw new RuntimeException( "The filesystem is reporting that the found source doesn't exist.", e );
        }
        
        return exists;
    }

    private DataSourceConfig getLeftSource()
    {
        DataSourceConfig sourceConfig = null;

        if (projectConfig.getInputs().getLeft() != null)
        {
            sourceConfig = projectConfig.getInputs().getLeft();
        }

        return sourceConfig;
    }

    private DataSourceConfig getRightSource()
    {
        DataSourceConfig sourceConfig = null;

        if (projectConfig.getInputs().getRight() != null)
        {
            sourceConfig = projectConfig.getInputs().getRight();
        }

        return sourceConfig;
    }

    private DataSourceConfig getBaseLine()
    {
        DataSourceConfig source = null;

        if (projectConfig.getInputs().getBaseline() != null)
        {
            source = projectConfig.getInputs().getBaseline();
        }

        return source;
    }

    private final ProjectConfig projectConfig;
    private boolean alreadySuspendedIndexes;
    private ProjectDetails projectDetails;
}
