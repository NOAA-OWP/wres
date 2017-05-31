package wres.io.reading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.stream.Stream;

import wres.io.config.specification.DirectorySpecification;
import wres.io.config.specification.FileSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.reading.BasicSource;
import wres.io.utilities.Database;

/**
 * @author Christopher Tubbs
 *
 */
public class ConfiguredLoader
{
    private final static String NEWLINE = System.lineSeparator();

    /**
     * 
     */
    public ConfiguredLoader(ProjectDataSpecification datasource) {
        this.datasource = datasource;
        this.lazyLoad = datasource.loadLazily();
    }
    
    public void load(){
        loadDataFromDirectories();
    }
    
    private void loadDataFromDirectories() {
        for (DirectorySpecification directory : datasource.getDirectories()) {
            if (directory.shouldLoadAllFiles()) {
                loadDirectory(directory.getPath());
            } else {
                saveFiles(directory);
            }
        }
    }
    
    private void loadDirectory(String path)
    {
        Path pathToDirectory = Paths.get(path);
        
        Stream<Path> files;
        try
        {
            files = Files.list(pathToDirectory);
            
            files.forEach(this::saveFile);
            
            files.close();
        }
        catch(IOException exception)
        {
            System.err.println("Data with the directory '" + path + "' could not be accessed for loading.");
            exception.printStackTrace();
        }
    }
    
    private void saveFiles(DirectorySpecification directory) {
        for (FileSpecification file : directory.get_files()) {
            saveFile(Paths.get(directory.getPath(), file.getPath()));
        }
    }
    
    private void saveFile(Path filePath) 
    {
        String absolutePath = filePath.toAbsolutePath().toString();
        try
        {
            if (!this.lazyLoad || !this.dataExists(absolutePath))
            {
                BasicSource source = ReaderFactory.getReader(absolutePath);
                if (datasource.isForecast()) {
                    source.save_forecast();
                }
                else {
                    source.save_observation();
                }
            }
        }
        catch(Exception exception)
        {
            System.err.println("The file at: '" + absolutePath + "' could not be loaded.");
            exception.printStackTrace();
        }
    }
    
    private boolean dataExists(String sourceName) throws SQLException {
        String script = "";
        
        script += "SELECT EXISTS (" + NEWLINE;
        script += "     SELECT 1" + NEWLINE;
        
        if (datasource.isForecast()) {
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

    private ProjectDataSpecification datasource;
    private boolean lazyLoad;
}
