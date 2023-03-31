package wres.config.yaml.components;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.Threshold;

/**
 * Wraps the canonical {@link wres.statistics.generated.Threshold.ThresholdOperator} with user-friendly names.
 * @author James Brown
 */
public enum ThresholdOperator
{
    /** Greater than. */
    @JsonProperty( "greater than" ) GREATER( "greater than" ),
    /** Less than. */
    @JsonProperty( "less than" ) LESS( "less than" ),
    /** Greater than or equal to. */
    @JsonProperty( "greater than or equal to" ) GREATER_EQUAL( "greater than or equal to" ),
    /** Less than or equal to. */
    @JsonProperty( "less than or equal to" ) LESS_EQUAL( "less than or equal to" ),
    /** Equal to. */
    @JsonProperty( "equal to" ) EQUAL( "equal to" );

    /** The user-friendly name. */
    private final String stringName;

    /**
     * @param canonical the canonical representation
     * @return the current representation
     */
    public static ThresholdOperator from( Threshold.ThresholdOperator canonical )
    {
        Objects.requireNonNull( canonical );
        return ThresholdOperator.valueOf( canonical.name() );
    }

    /**
     * Creates an instance from a user-friendly name, either represented as an enum name or a user-facing string.
     * @param stringName the user-friendly name or equivalent enum
     * @return the current representation
     */
    public static ThresholdOperator from( String stringName )
    {
        Objects.requireNonNull( stringName );

        String friendlyName = DeclarationFactory.getFriendlyName( stringName );
        Optional<ThresholdOperator> optional = Arrays.stream( ThresholdOperator.values() )
                                                     .filter( next -> next.toString()
                                                                          .equals( friendlyName ) )
                                                     .findFirst();
        return optional.orElseThrow( () -> new IllegalArgumentException( "Could not find an operator for name '"
                                                                         + stringName
                                                                         + "'" ) );
    }

    /**
     * @return the canonical representation
     */
    public Threshold.ThresholdOperator canonical()
    {
        return Threshold.ThresholdOperator.valueOf( this.name() );
    }

    @Override
    public String toString()
    {
        return stringName;
    }

    /**
     * @param stringName the user-friendly name
     */
    ThresholdOperator( String stringName )
    {
        this.stringName = stringName;
    }
}
