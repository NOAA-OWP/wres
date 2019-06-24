package wres.datamodel.sampledata;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with incorrect metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetadataException extends DataException
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -1559780940246528464L;
    
    /**
     * Constructs an {@link MetadataException} with no message.
     */

    public MetadataException()
    {
        super();
    }

    /**
     * Constructs a {@link MetadataException} with the specified message.
     * 
     * @param message the message.
     */

    public MetadataException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetadataException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetadataException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
