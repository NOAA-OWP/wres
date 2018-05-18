package wres.io.writing;

import java.util.Objects;

import wres.config.ProjectConfigException;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;

/**
 * Utility methods to help with writing metric outputs to file.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */
public class WriterHelper
{

    /**
     * Validates the project configuration for writing. Throws an exception if the configuration is invalid.
     * 
     * @param projectConfig the project configuration
     * @throws NullPointerException if the input is null
     * @throws ProjectConfigException if the project is not correctly configured for writing numerical output
     */

    public static void validateProjectForWriting( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration when writing outputs." );

        if ( Objects.isNull( projectConfig.getOutputs() )
             || Objects.isNull( projectConfig.getOutputs().getDestination() )
             || projectConfig.getOutputs().getDestination().isEmpty() )
        {
            throw new ProjectConfigException( projectConfig.getOutputs(),
                                              ConfigHelper.OUTPUT_CLAUSE_BOILERPLATE );
        }
    }

    /**
     * Do not construct.
     */

    private WriterHelper()
    {
    }

}
