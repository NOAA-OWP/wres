package wres.io.reading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import wres.io.concurrency.ObservationSaver;
import wres.io.config.ConfigHelper;
import wres.io.utilities.Database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Christopher Tubbs
 *
 */
public class SourceLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);

    private static final String NEWLINE = System.lineSeparator();

    /**
     *
     */
    public SourceLoader (ProjectConfig projectConfig) {
        this.projectConfig = projectConfig;
    }

    /**
     * Ingest data
     * @return List of Future file ingest results
     * @throws IOException but swallows SQLException
     */
    public List<Future> load() throws IOException
    {
        List<Future> savingFiles = new ArrayList<>();

        savingFiles.addAll(loadConfig(getLeftSource()));
        savingFiles.addAll(loadConfig(getRightSource()));
        savingFiles.addAll(loadConfig(getBaseLine()));

        return savingFiles;
    }

    private List<Future> loadConfig(DataSourceConfig config) throws IOException {
        List<Future> savingFiles = new ArrayList<>();

        if (config != null) {
            for (DataSourceConfig.Source source : config.getSource()) {
                Path sourcePath = Paths.get(source.getValue());

                if (Files.exists(sourcePath) && Files.isDirectory(sourcePath)) {

                    List<Future> tasks = loadDirectory(sourcePath, source, config);

                    if (tasks != null)
                    {
                        savingFiles.addAll(tasks);
                    }
                }
                else if (Files.exists(sourcePath) && Files.isRegularFile(sourcePath)) {
                    Future task = saveFile(sourcePath, config);

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
                    Future task = saveFile(path, dataSourceConfig);

                    if (task != null)
                    {
                        results.add(task);
                    }
                }
            });

            files.close();
        }
        catch (IOException e) {
            e.printStackTrace();
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
    private Future saveFile(Path filePath, DataSourceConfig dataSourceConfig)
    {
        String absolutePath = filePath.toAbsolutePath().toString();
        try
        {
            if (!this.dataExists(absolutePath, dataSourceConfig))
            {
                if (ConfigHelper.isForecast(dataSourceConfig)) {
                    LOGGER.info("Loading {} as forecast data...", absolutePath);
                    return Executor.execute(new ForecastSaver(absolutePath, dataSourceConfig));
                }
                else {
                    LOGGER.info("Loading {} as Observation data...");
                    return Executor.execute(new ObservationSaver(absolutePath, dataSourceConfig));
                }
            }
            else
            {
                LOGGER.info("Data will not be loaded from {}. That data is already in the database.", absolutePath);
            }
        }
        catch(SQLException exception)
        {
            LOGGER.error("The file at: '{}' could not be loaded.", absolutePath);
            LOGGER.error("Exception was: ", exception);
        }
        return null;
    }


    private boolean dataExists(String sourceName, DataSourceConfig dataSourceConfig) throws SQLException {
        StringBuilder script = new StringBuilder();
        
        script.append("SELECT EXISTS (").append(NEWLINE);
        script.append("     SELECT 1").append(NEWLINE);
        
        if (ConfigHelper.isForecast(dataSourceConfig))
        {
            script.append("     FROM wres.Forecast F").append(NEWLINE);
            script.append("     INNER JOIN wres.ForecastSource SL").append(NEWLINE);
            script.append("         ON SL.forecast_id = F.forecast_id").append(NEWLINE);
            script.append("     INNER JOIN wres.ForecastEnsemble FE").append(NEWLINE);
            script.append("         ON FE.forecast_id = F.forecast_id").append(NEWLINE);
            script.append("     INNER JOIN wres.VariablePosition VP").append(NEWLINE);
            script.append("         ON VP.variableposition_id = FE.variableposition_id").append(NEWLINE);
        }
        else
        {
            script.append("     FROM wres.Observation SL").append(NEWLINE);
            script.append("     INNER JOIN wres.VariablePosition VP").append(NEWLINE);
            script.append("         ON VP.variableposition_id = SL.variableposition_id").append(NEWLINE);
        }
        
        script.append("     INNER JOIN wres.Source S").append(NEWLINE);
        script.append("         ON S.source_id = SL.source_id").append(NEWLINE);
        script.append("     INNER JOIN wres.Variable V").append(NEWLINE);
        script.append("         ON VP.variable_id = V.variable_id").append(NEWLINE);
        script.append("     WHERE S.path = '").append(sourceName).append("'").append(NEWLINE);
        script.append("         AND V.variable_name = '").append(dataSourceConfig.getVariable().getValue()).append("'").append(NEWLINE);
        script.append(");");
        
        return Database.getResult(script.toString(), "exists");
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
}
