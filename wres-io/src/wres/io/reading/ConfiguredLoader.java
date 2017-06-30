package wres.io.reading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import wres.io.concurrency.ObservationSaver;
import wres.io.config.specification.DirectorySpecification;
import wres.io.config.specification.FileSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;

/**
 * @author Christopher Tubbs
 *
 */
public class ConfiguredLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredLoader.class);

    private static final String NEWLINE = System.lineSeparator();

    /**
     * 
     */
    public ConfiguredLoader(ProjectDataSpecification datasource) {
        this.datasource = datasource;
        this.lazyLoad = datasource.loadLazily();
    }

    /**
     * Ingest data
     * @return List of Future file ingest results
     * @throws IOException but swallows SQLException
     */
    public List<Future> load() throws IOException
    {
        return loadDataFromDirectories();
    }

    private List<Future> loadDataFromDirectories() throws IOException
    {
        List<Future> results = new ArrayList<>();
        for (DirectorySpecification directory : datasource.getDirectories())
        {
            if (directory.shouldLoadAllFiles())
            {
                results.addAll(loadDirectory(directory.getPath()));
            }
            else
            {
                results.addAll(saveFiles(directory));
            }
        }
        return Collections.unmodifiableList(results);
    }

    private List<Future> loadDirectory(Path pathToDirectory) throws IOException
    {
        List<Future> results = new ArrayList<>();

        Stream<Path> files;
        try
        {
            files = Files.list(pathToDirectory);

            files.filter(Objects::nonNull)
                 .map(f -> saveFile(f))
                 .filter(Objects::nonNull)
                 .forEach(results::add);

            files.close();
        }
        catch(IOException exception)
        {
            LOGGER.error("Data with the directory '{}' could not be accessed for loading.",
                         pathToDirectory);
            LOGGER.error("Exception was: ", exception);
            throw exception;
        }
        return Collections.unmodifiableList(results);
    }

    private List<Future> saveFiles(DirectorySpecification directory) throws IOException
    {
        List<Future> results = new ArrayList<>();


        for (FileSpecification file : directory.getFiles())
        {
            Future f = saveFile(directory.getPath().resolve(file.getPath()));
            if (f != null)
            {
                results.add(f);
            }
        }
        return Collections.unmodifiableList(results);
    }

    /**
     * saveFile returns Future on success, null in several cases.
     * Caller must expect null and handle it appropriately.
     *
     * @param filePath
     * @return Future if task was created, null otherwise.
     */
    private Future saveFile(Path filePath)
    {
        String absolutePath = filePath.toAbsolutePath().toString();
        try
        {
            if (!this.lazyLoad || !this.dataExists(absolutePath))
            {
                if (datasource.isForecast()) {
                    return Executor.execute(new ForecastSaver(absolutePath));
                }
                else {
                    return Executor.execute(new ObservationSaver(absolutePath));
                }
            }
        }
        catch(SQLException exception)
        {
            LOGGER.error("The file at: '{}' could not be loaded.", absolutePath);
            LOGGER.error("Exception was: ", exception);
        }
        return null;
    }


    private boolean dataExists(String sourceName) throws SQLException {
        String script = "";
        
        script += "SELECT EXISTS (" + NEWLINE;
        script += "     SELECT 1" + NEWLINE;
        
        if (datasource.isForecast())
        {
            script += "     FROM wres.Forecast F" + NEWLINE;
            script += "     INNER JOIN wres.ForecastSource SL" + NEWLINE;
            script += "         ON SL.forecast_id = F.forecast_id" + NEWLINE;
        }
        else
        {
            script += "     FROM wres.Observation SL" + NEWLINE;
        }
        
        script += "     INNER JOIN wres.Source S" + NEWLINE;
        script += "         ON S.source_id = SL.source_id" + NEWLINE;
        script += "     WHERE S.path = '" + sourceName + "'" + NEWLINE;
        script += ");";
        
        return Database.getResult(script, "exists");
    }

    private final ProjectDataSpecification datasource;
    private final boolean lazyLoad;
}
