package wres.config.yaml.components;

/**
 * The orientation of a dataset within an evaluation.
 * @author James Brown
 */
public enum DatasetOrientation
{
    /** A left or observed dataset. */
    LEFT( "observed" ),
    /** A right or predicted dataset. */
    RIGHT( "predicted" ),
    /** A baseline dataset. */
    BASELINE( "baseline" );

    private final String stringName;

    DatasetOrientation( String stringName )
    {
        this.stringName = stringName;
    }

    @Override
    public String toString()
    {
        return stringName;
    }
}
