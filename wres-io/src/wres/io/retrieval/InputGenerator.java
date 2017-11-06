package wres.io.retrieval;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricInput;
import wres.io.utilities.NoDataException;
import wres.util.NotImplementedException;
import wres.util.Strings;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<MetricInput<?>>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InputGenerator.class);

    public InputGenerator (ProjectConfig projectConfig, Feature feature)
    {
        this.projectConfig = projectConfig;
        this.feature = feature;
    }

    private final ProjectConfig projectConfig;
    private final Feature feature;

    @Override
    public Iterator<Future<MetricInput<?>>> iterator()
    {
        BackToBackMetricInputIterator iterator = null;
        try {
            iterator =  new BackToBackMetricInputIterator( this.projectConfig,
                                                           this.feature);
        }
        catch (SQLException | NotImplementedException | InvalidPropertiesFormatException e)
        {
            LOGGER.error("A MetricInputIterator could not be created.");
            LOGGER.error(Strings.getStackTrace(e));
        }
        catch ( NoDataException e )
        {
            LOGGER.error("A MetricInputIterator could not be created. " + ""
                         + "There's no data to iterate over.");
            LOGGER.error(Strings.getStackTrace( e ));
        }
        return iterator;
    }


}
