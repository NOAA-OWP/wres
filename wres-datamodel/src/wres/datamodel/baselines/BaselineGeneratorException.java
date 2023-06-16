package wres.datamodel.baselines;

import java.io.Serial;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with the functional generation of a baseline time-series.
 *
 * @author James Brown
 */

public class BaselineGeneratorException extends DataException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = -8585419352611659835L;

    /**
     * Constructs a {@link BaselineGeneratorException} with the specified message.
     *
     * @param message the message.
     */

    public BaselineGeneratorException( final String message )
    {
        super( message );
    }
}
