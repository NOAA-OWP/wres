package wres.vis.charts;

import java.io.Serial;

/**
 * Exception that indicates a chart could not be constructed
 */

public class ChartBuildingException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = 5710493568160830259L;

    /**
     * Constructs a {@link ChartBuildingException} with the specified message.
     * 
     * @param message the message.
     */

    ChartBuildingException( final String message )
    {
        super( message );
    }
}
