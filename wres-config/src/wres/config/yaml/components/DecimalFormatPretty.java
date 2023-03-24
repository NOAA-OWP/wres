package wres.config.yaml.components;

import java.text.DecimalFormat;

/**
 * Implementation of a {@link DecimalFormat} that has a pretty string representation.
 */
public class DecimalFormatPretty extends DecimalFormat
{
    /**
     * Create an instance
     * @param format the format
     */
    public DecimalFormatPretty( String format )
    {
        super( format );
    }

    @Override
    public String toString()
    {
        return this.toPattern();
    }
}
