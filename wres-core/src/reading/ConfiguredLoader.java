/**
 * 
 */
package reading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.stream.Stream;

import config.specification.DirectorySpecification;
import config.specification.FileSpecification;
import config.specification.ProjectDataSpecification;
import reading.BasicSource;
import util.Database;

/**
 * @author Christopher Tubbs
 *
 */
public class ConfiguredLoader
{
    private final static String newline = System.lineSeparator();

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
            
            files.forEach((Path filePath) -> {
                saveFile(filePath);
            });
            
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
        
        script += "SELECT EXISTS (" + newline;
        script += "     SELECT 1" + newline;
        
        if (datasource.isForecast()) {
            script += "     FROM wres.Forecast F" + newline;
            script += "     INNER JOIN wres.ForecastSource SL" + newline;
            script += "         ON SL.forecast_id = F.forecast_id" + newline;
        }
        else
        {
            script += "     FROM wres.Observation SL" + newline;
        }
        
        script += "     INNER JOIN wres.Source S" + newline;
        script += "         ON S.source_id = SL.source_id" + newline;
        script += "     WHERE S.path = '" + sourceName + "'" + newline;
        script += ");";
        
        return Database.getResult(script, "exists");
    }

    private ProjectDataSpecification datasource;
    private boolean lazyLoad;
}
