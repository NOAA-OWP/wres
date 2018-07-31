package wres.datamodel.metadata;

import java.util.Objects;

/**
 * An immutable store of metadata associated with metric inputs and outputs.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Metadata
{

    /**
     * The dimension or measurement units associated with the data.
     */

    private final MeasurementUnit dimension;

    /**
     * An optional dataset identifier, may be null.
     */

    private final DatasetIdentifier identifier;

    /**
     * An optional time window associated with the data, may be null.
     */

    private final TimeWindow timeWindow;


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
     * @param dim the dimension
     * @return a {@link Metadata} object
     */

    public static Metadata of( final MeasurementUnit dim )
    {
        return Metadata.of( dim, null, null );
    }

    /**
     * Build a {@link Metadata} object with a prescribed {@link MeasurementUnit} and an optional {@link DatasetIdentifier}.
     * 
     * @param dim the dimension
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link Metadata} object
     */

    public static Metadata of( final MeasurementUnit dim, final DatasetIdentifier identifier )
    {
        return Metadata.of( dim, identifier, null );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link MeasurementUnit}.
     * 
     * @param input the source metadata
     * @param dim the new dimension
     * @return a {@link Metadata} object
     */

    public static Metadata of( final Metadata input, final MeasurementUnit dim )
    {
        return Metadata.of( dim, input.getIdentifier(), input.getTimeWindow() );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link TimeWindow}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link Metadata} object
     */

    public static Metadata of( final Metadata input, final TimeWindow timeWindow )
    {
        return Metadata.of( input.getDimension(), input.getIdentifier(), timeWindow );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param dimension a required dimension
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static Metadata of( MeasurementUnit dimension, DatasetIdentifier identifier, TimeWindow timeWindow )
    {
        return new Metadata( dimension, identifier, timeWindow );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof Metadata ) )
        {
            return false;
        }
        final Metadata p = (Metadata) o;
        boolean returnMe = this.equalsWithoutTimeWindow( p ) && this.hasTimeWindow() == p.hasTimeWindow();
        if ( hasTimeWindow() )
        {
            returnMe = returnMe && this.getTimeWindow().equals( p.getTimeWindow() );
        }
        return returnMe;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( getDimension(), hasIdentifier(), hasTimeWindow(), getIdentifier(), getTimeWindow() );
    }

    @Override
    public String toString()
    {
        final StringBuilder b = new StringBuilder();
        if ( hasIdentifier() )
        {
            String appendMe = identifier.toString();
            appendMe = appendMe.replaceAll( "]", "," );
            b.append( appendMe );
        }
        else
        {
            b.append( "[" );
        }
        if ( hasTimeWindow() )
        {
            b.append( timeWindow ).append( "," );
        }
        b.append( dimension ).append( "]" );
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
     * Returns <code>true</code> if the input is equal to the current {@link Metadata} without considering the 
     * {@link TimeWindow}.
     * 
     * @param input the input metadata
     * @return true if the input is equal to the current metadata, without considering the time window
     */
    public boolean equalsWithoutTimeWindow( final Metadata input )
    {
        if ( Objects.isNull( input ) )
        {
            return false;
        }
        boolean returnMe = input.getDimension().equals( getDimension() ) && hasIdentifier() == input.hasIdentifier();
        if ( hasIdentifier() )
        {
            returnMe = returnMe && getIdentifier().equals( input.getIdentifier() );
        }
        return returnMe;
    }

    /**
     * Returns the dimension associated with the metric.
     * 
     * @return the dimension
     */

    public MeasurementUnit getDimension()
    {
        return this.dimension;
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
     * A hidden constructor.
     * 
     * @param dimension a required dimension
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @throws NullPointerException if the dimension is null
     */

    Metadata( final MeasurementUnit dimension, final DatasetIdentifier identifier, final TimeWindow timeWindow )
    {
        Objects.requireNonNull( "Specify a non-null dimension from which to construct the metadata." );

        this.dimension = dimension;
        this.identifier = identifier;
        this.timeWindow = timeWindow;
    }

}
