package wres.config;

import wres.config.generated.ProbabilityThresholdConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */
public class ProjectConfigs
{
    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
    }

    /**
     * Parse and validate probabilities from a given ProbabilityThresholdConfig
     * @param p the threshold configuration to parse
     * @return the list of Doubles found
     * @throws ProjectConfigException when unparseable configuration is given
     * @throws NullPointerException when p is null
     */
    public static List<Double> parseProbabilities( ProbabilityThresholdConfig p )
            throws ProjectConfigException
    {
        Objects.requireNonNull( p, "Config element passed must not be null" );

        List<Double> result = new ArrayList<>();
        try
        {
            String[] probabilities = p.getProbabilities()
                                      .split( "," );

            for ( String value : probabilities )
            {
                Double doubleValue = Double.parseDouble( value );

                // May as well validate while doing this
                if ( doubleValue < 0.0 || doubleValue > 1.0 )
                {
                    String message = "Please set probabilities to values"
                            + "between 0.0 and 1.0.";
                    throw new ProjectConfigException( p, message );
                }

                result.add( doubleValue );
            }
        }
        catch ( NumberFormatException s )
        {
            throw new ProjectConfigException( p,
                                              "Failed to parse probabilities.",
                                              s );
        }
        return result;
    }
}

