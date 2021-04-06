package wres.datamodel.pools;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with incorrect metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SampleMetadataException extends DataException
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -1559780940246528464L;
    
    /**
     * Constructs an {@link SampleMetadataException} with no message.
     */

    public SampleMetadataException()
    {
        super();
    }

    /**
     * Constructs a {@link SampleMetadataException} with the specified message.
     * 
     * @param message the message.
     */

    public SampleMetadataException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link SampleMetadataException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public SampleMetadataException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
