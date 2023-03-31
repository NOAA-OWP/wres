package wres.config.xml;

import java.io.Serial;

import com.sun.xml.bind.Locatable;

/**
 * An unchecked exception associated with metric configuration.
 * 
 * @author James Brown
 */

public final class MetricConfigException extends ProjectConfigException
{
    /**
     * Serial identifier.
     */

    @Serial
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
