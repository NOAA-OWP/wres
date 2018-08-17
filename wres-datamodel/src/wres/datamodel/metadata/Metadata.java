package wres.datamodel.metadata;

import java.util.Objects;

import wres.config.generated.ProjectConfig;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * An immutable store of metadata associated with metric inputs and outputs.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Metadata
{

    /**
     * Error message for null input.
     */

    private static final String NULL_INPUT_ERROR = "Specify non-null input from which to build the metadata.";

    /**
     * The measurement unit associated with the data.
     */

    private final MeasurementUnit unit;

    /**
     * An optional dataset identifier, may be null.
     */

    private final DatasetIdentifier identifier;

    /**
     * An optional time window associated with the data, may be null.
     */

    private final TimeWindow timeWindow;

    /**
     * An optional set of thresholds associated with the data, may be null.
     */

    private final OneOrTwoThresholds thresholds;

    /**
     * The optional {@link ProjectConfig} associated with the metadata, may be null.
     */

    private final ProjectConfig projectConfig;

    /**
     * Build a {@link Metadata} object with a default {@link MeasurementUnit}.
     * 
     * @return a {@link Metadata} object
     */

    public static Metadata of()
    {
        return Metadata.of( MeasurementUnit.of() );
    }

    /**
     * Build a {@link Metadata} object with a sample size and a prescribed {@link MeasurementUnit}.
     * 
     * @param unit the required measurement unit
     * @return a {@link Metadata} object
     */

    public static Metadata of( final MeasurementUnit unit )
    {
        return Metadata.of( unit, null );
    }

    /**
     * Build a {@link Metadata} object with a prescribed {@link MeasurementUnit} and an optional {@link DatasetIdentifier}.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link Metadata} object
     */

    public static Metadata of( final MeasurementUnit unit, final DatasetIdentifier identifier )
    {
        return Metadata.of( unit, identifier, null );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static Metadata of( final MeasurementUnit unit, DatasetIdentifier identifier, final TimeWindow timeWindow )
    {
        return Metadata.of( unit, identifier, timeWindow, null, null );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param projectConfig the optional project configuration
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static Metadata of( final MeasurementUnit unit,
                               DatasetIdentifier identifier,
                               final TimeWindow timeWindow,
                               final ProjectConfig projectConfig )
    {
        return Metadata.of( unit, identifier, timeWindow, null, projectConfig );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static Metadata of( final MeasurementUnit unit,
                               final DatasetIdentifier identifier,
                               final TimeWindow timeWindow,
                               final OneOrTwoThresholds thresholds )
    {
        return new Metadata( unit, identifier, timeWindow, thresholds, null );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @param projectConfig the optional project configuration
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static Metadata of( final MeasurementUnit unit,
                               final DatasetIdentifier identifier,
                               final TimeWindow timeWindow,
                               final OneOrTwoThresholds thresholds,
                               final ProjectConfig projectConfig )
    {
        return new Metadata( unit, identifier, timeWindow, thresholds, projectConfig );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link MeasurementUnit}.
     * 
     * @param input the source metadata
     * @param unit the required measurement unit
     * @return a {@link Metadata} object
     * @throws NullPointerException if the input is null
     */

    public static Metadata of( final Metadata input, final MeasurementUnit unit )
    {
        Objects.requireNonNull( input, NULL_INPUT_ERROR );

        return Metadata.of( unit,
                            input.getIdentifier(),
                            input.getTimeWindow(),
                            input.getThresholds(),
                            input.getProjectConfig() );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param thresholds the thresholds
     * @return a {@link Metadata} object
     * @throws NullPointerException if the input is null
     */

    public static Metadata of( final Metadata input, final OneOrTwoThresholds thresholds )
    {
        Objects.requireNonNull( input, NULL_INPUT_ERROR );

        return Metadata.of( input.getMeasurementUnit(),
                            input.getIdentifier(),
                            input.getTimeWindow(),
                            thresholds,
                            input.getProjectConfig() );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link TimeWindow}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link Metadata} object
     * @throws NullPointerException if the input is null
     */

    public static Metadata of( final Metadata input, final TimeWindow timeWindow )
    {
        Objects.requireNonNull( input, NULL_INPUT_ERROR );

        return Metadata.of( input.getMeasurementUnit(),
                            input.getIdentifier(),
                            timeWindow,
                            input.getThresholds(),
                            input.getProjectConfig() );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof Metadata ) )
        {
            return false;
        }
        final Metadata p = (Metadata) o;
        boolean returnMe = this.equalsWithoutTimeWindowOrThresholds( p ) && this.hasTimeWindow() == p.hasTimeWindow()
                           && this.hasThresholds() == p.hasThresholds();

        if ( returnMe && hasTimeWindow() )
        {
            returnMe = this.getTimeWindow().equals( p.getTimeWindow() );
        }

        if ( returnMe && hasThresholds() )
        {
            returnMe = this.getThresholds().equals( p.getThresholds() );
        }

        return returnMe;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.hasIdentifier(),
                             this.hasTimeWindow(),
                             this.hasThresholds(),
                             this.hasProjectConfig(),
                             this.getMeasurementUnit(),
                             this.getIdentifier(),
                             this.getTimeWindow(),
                             this.getThresholds(),
                             this.getProjectConfig() );
    }

    @Override
    public String toString()
    {
        final StringBuilder b = new StringBuilder();
        if ( hasIdentifier() )
        {
            String appendMe = this.identifier.toString();
            appendMe = appendMe.replaceAll( "\\]", "," );
            appendMe = appendMe.replaceAll( "\\[", "(" );
            b.append( appendMe );
        }
        else
        {
            b.append( "(" );
        }
        if ( this.hasTimeWindow() )
        {
            b.append( this.getTimeWindow() ).append( "," );
        }
        if ( this.hasThresholds() )
        {
            b.append( this.getThresholds() ).append( "," );
        }

        b.append( this.getMeasurementUnit() ).append( ")" );

        return b.toString();
    }


    /**
     * Returns <code>true</code> if {@link #getIdentifier()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getIdentifier()} returns non-null, false otherwise.
     */
    public boolean hasIdentifier()
    {
        return Objects.nonNull( this.getIdentifier() );
    }

    /**
     * Returns <code>true</code> if {@link #getTimeWindow()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    public boolean hasTimeWindow()
    {
        return Objects.nonNull( this.getTimeWindow() );
    }

    /**
     * Returns <code>true</code> if {@link #getThresholds()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getThresholds()} returns non-null, false otherwise.
     */
    public boolean hasThresholds()
    {
        return Objects.nonNull( this.getThresholds() );
    }

    /**
     * Returns <code>true</code> if {@link #getProjectConfig()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getProjectConfig()} returns non-null, false otherwise.
     */
    public boolean hasProjectConfig()
    {
        return Objects.nonNull( this.getProjectConfig() );
    }

    /**
     * Returns <code>true</code> if the input is equal to the current {@link Metadata} without considering the 
     * {@link #getTimeWindow()} or {@link #getThresholds()}.
     * 
     * @param input the input metadata
     * @return true if the input is equal to the current metadata, without considering the time window or thresholds
     */
    public boolean equalsWithoutTimeWindowOrThresholds( final Metadata input )
    {
        if ( Objects.isNull( input ) )
        {
            return false;
        }
        boolean returnMe =
                input.getMeasurementUnit().equals( this.getMeasurementUnit() )
                           && this.hasIdentifier() == input.hasIdentifier()
                           && this.hasProjectConfig() == input.hasProjectConfig();

        if ( hasIdentifier() )
        {
            returnMe = returnMe && this.getIdentifier().equals( input.getIdentifier() );
        }

        if ( hasProjectConfig() )
        {
            returnMe = returnMe && this.getProjectConfig().equals( input.getProjectConfig() );
        }


        return returnMe;
    }

    /**
     * Returns the measurement unit associated with the metric.
     * 
     * @return the measurement unit
     */

    public MeasurementUnit getMeasurementUnit()
    {
        return this.unit;
    }

    /**
     * Returns an optional dataset identifier or null.
     * 
     * @return an identifier or null
     */

    public DatasetIdentifier getIdentifier()
    {
        return this.identifier;
    }

    /**
     * Returns a {@link TimeWindow} associated with the metadata or null.
     * 
     * @return a lead time or null
     */

    public TimeWindow getTimeWindow()
    {
        return this.timeWindow;
    }

    /**
     * Returns a {@link OneOrTwoThresholds} associated with the metadata or null.
     * 
     * @return a set of thresholds or null
     */

    public OneOrTwoThresholds getThresholds()
    {
        return this.thresholds;
    }

    /**
     * Returns a {@link ProjectConfig} associated with the metadata or null.
     * 
     * @return the project declaration or null
     */

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * A hidden constructor.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @param projectConfig the optional project configuration
     * @throws NullPointerException if the dimension is null
     */

    Metadata( final MeasurementUnit unit,
              final DatasetIdentifier identifier,
              final TimeWindow timeWindow,
              final OneOrTwoThresholds thresholds,
              final ProjectConfig projectConfig )
    {
        Objects.requireNonNull( "Specify a non-null measurement unit from which to construct the metadata." );

        this.unit = unit;
        this.identifier = identifier;
        this.timeWindow = timeWindow;
        this.thresholds = thresholds;
        this.projectConfig = projectConfig;
    }

}
