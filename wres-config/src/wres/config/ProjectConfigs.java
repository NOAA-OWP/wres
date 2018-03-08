package wres.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */
public class ProjectConfigs
{
    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
    }

    private static final String NON_NULL = "Config element passed must not be null";

    /**
     * Parse and validate the {@link ThresholdsConfig} element when the thresholds are internally defined.
     *
     * @param v the value threshold configuration to parse and validate
     * @return the list of Doubles found in the tag containing values
     * @throws ProjectConfigException if the values could not be parsed as {@link Double}
     * @throws NullPointerException when v is null
     */
    public static List<Double> parseValues( ThresholdsConfig v )
            throws ProjectConfigException
    {
        Objects.requireNonNull( v, NON_NULL );

        List<Double> result = new ArrayList<>();

        try
        {
            // Cannot read externally sourced thresholds
            Object source = v.getCommaSeparatedValuesOrSource();
            if ( source instanceof ThresholdsConfig.Source )
            {
                throw new ProjectConfigException( v, "Cannot read externally sourced thresholds." );
            }

            String[] values = ( (String) source ).split( "," );

            for ( String value : values )
            {
                Double doubleValue = Double.parseDouble( value );

                // Validate
                if ( ( v.getType() == ThresholdType.PROBABILITY
                       || v.getType() == ThresholdType.PROBABILITY_CLASSIFIER )
                     && ( doubleValue < 0.0 || doubleValue > 1.0 ) )
                {
                    String message = "Please set probabilities to values"
                                     + " between 0.0 and 1.0.";
                    throw new ProjectConfigException( v, message );
                }

                result.add( doubleValue );
            }
        }
        catch ( NumberFormatException s )
        {
            throw new ProjectConfigException( v, "Failed to parse values.", s );
        }

        // OK to avoid copying since this scope created result.
        return Collections.unmodifiableList( result );
    }

}

