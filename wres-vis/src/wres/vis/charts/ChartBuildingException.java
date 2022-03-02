package wres.vis.charts;

/**
 * Exception that indicates a chart could not be constructed
 */

public class ChartBuildingException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 5710493568160830259L;

    /**
     * Constructs an {@link ChartBuildingException} with no message.
     */

    ChartBuildingException()
    {
        super();
    }

    /**
     * Constructs a {@link ChartBuildingException} with the specified message.
     * 
     * @param message the message.
     */

    ChartBuildingException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link ChartBuildingException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    ChartBuildingException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
