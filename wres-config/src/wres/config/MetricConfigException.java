package wres.config;

import com.sun.xml.bind.Locatable;

/**
 * A checked exception associated with metric configuration.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetricConfigException extends ProjectConfigException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 6790246291277821024L;

    /**
     * Constructs a {@link MetricConfigException} with the specified message.
     * 
     * @param origin the origin
     * @param message the message.
     */

    public MetricConfigException( final Locatable origin, final String message )
    {
        super( origin, message );
    }

    /**
     * Constructs a {@link MetricConfigException} with the specified message.
     * 
     * @param origin the origin
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricConfigException( final Locatable origin, final String message, final Throwable cause )
    {
        super( origin, message, cause );
    }

    /**
     * Constructs a {@link MetricConfigException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricConfigException( final String message )
    {
        super( null, message );
    }

    /**
     * Constructs a {@link MetricConfigException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricConfigException( final String message, final Throwable cause )
    {
        super( null, message, cause );
    }

}
