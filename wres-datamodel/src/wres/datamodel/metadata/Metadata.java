package wres.datamodel.metadata;

import java.util.Objects;

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

        return Metadata.of( unit, input.getIdentifier(), input.getTimeWindow() );
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
                            thresholds );
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

        return Metadata.of( input.getMeasurementUnit(), input.getIdentifier(), timeWindow );
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

    public static Metadata of( MeasurementUnit unit, DatasetIdentifier identifier, TimeWindow timeWindow )
    {
        return Metadata.of( unit, identifier, timeWindow, null );
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

    public static Metadata of( MeasurementUnit unit,
                               DatasetIdentifier identifier,
                               TimeWindow timeWindow,
                               OneOrTwoThresholds thresholds )
    {
        return new Metadata( unit, identifier, timeWindow, thresholds );
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
        return Objects.hash( this.getMeasurementUnit(),
                             this.hasIdentifier(),
                             this.hasTimeWindow(),
                             this.hasThresholds(),
                             this.getIdentifier(),
                             this.getTimeWindow(),
                             this.getThresholds() );
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
     * Returns true if {@link #getIdentifier()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getIdentifier()} returns non-null, false otherwise.
     */
    public boolean hasIdentifier()
    {
        return Objects.nonNull( getIdentifier() );
    }

    /**
     * Returns true if {@link #getTimeWindow()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    public boolean hasTimeWindow()
    {
        return Objects.nonNull( getTimeWindow() );
    }

    /**
     * Returns true if {@link #getThresholds()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getThresholds()} returns non-null, false otherwise.
     */
    public boolean hasThresholds()
    {
        return Objects.nonNull( getThresholds() );
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
                input.getMeasurementUnit().equals( getMeasurementUnit() ) && hasIdentifier() == input.hasIdentifier();
        if ( hasIdentifier() )
        {
            returnMe = returnMe && getIdentifier().equals( input.getIdentifier() );
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
     * A hidden constructor.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @throws NullPointerException if the dimension is null
     */

    Metadata( final MeasurementUnit unit,
              final DatasetIdentifier identifier,
              final TimeWindow timeWindow,
              final OneOrTwoThresholds thresholds )
    {
        Objects.requireNonNull( "Specify a non-null measurement unit from which to construct the metadata." );

        this.unit = unit;
        this.identifier = identifier;
        this.timeWindow = timeWindow;
        this.thresholds = thresholds;
    }

}
