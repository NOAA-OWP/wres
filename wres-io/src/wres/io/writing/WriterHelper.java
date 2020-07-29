package wres.io.writing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.statistics.Statistic;
import wres.io.config.ConfigHelper;

/**
 * Utility methods to help with writing metric outputs to file.
 * 
 * @author james.brown@hydrosolved.com
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
     * Returns a map of statistics grouped by the {@link LeftOrRightOrBaseline}.
     * 
     * @param <T> the type of statistic
     * @param input the input list of statistics
     * @return the statistics grouped by context
     */

    public static <T extends Statistic<?>> Map<LeftOrRightOrBaseline, List<T>>
            getStatisticsGroupedByContext( List<T> input )
    {
        // Move to WriteHelper                
        Map<LeftOrRightOrBaseline, List<T>> groups =
                input.stream()
                     .collect( Collectors.groupingBy( a -> a.getMetadata()
                                                            .getIdentifier()
                                                            .getLeftOrRightOrBaseline() ) );

        return Collections.unmodifiableMap( groups );
    }
    
    /**
     * Do not construct.
     */

    private WriterHelper()
    {
    }

}
