package wres.config;

import java.util.Comparator;
import java.util.Objects;

import wres.config.generated.ProjectConfig;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */

public class ProjectConfigs
{

    /**
     * Returns <code>true</code> if the input configuration has time-series metrics, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has time-series metrics, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTimeSeriesMetrics( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project declaration." );

        return projectConfig.getMetrics().stream().anyMatch( next -> !next.getTimeSeriesMetric().isEmpty() );
    }
    
    /**
     * Compares the input instances of {@link ProjectConfig}. Returns a negative, zero, or positive value when the first
     * input is less than, equal to, or greater than the second input, respectively. This is a minimal implementation
     * that is consistent with {@link Object#equals(Object)} and otherwise compares the inputs according to the value
     * of the {@link ProjectConfig#getName()} alone.
     * 
     * @param first the first input
     * @param second the second input
     * @return a negative, zero or positive integer when the first input is less than, equal to, or greater than
     *            the second input
     * @throws NullPointerException if either input is null
     */

    public static int compare( ProjectConfig first, ProjectConfig second )
    {
        Objects.requireNonNull( first, "The first input is null, which is not allowed." );

        Objects.requireNonNull( second, "The second input is null, which is not allowed." );

        if ( first.equals( second ) )
        {
            return 0;
        }

        // Null friendly natural order on project name
        return Objects.compare( first.getName(), second.getName(), Comparator.nullsFirst( Comparator.naturalOrder() ) );
    }

    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
    }

}

