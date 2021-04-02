package wres.statistics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import wres.statistics.generated.Outputs;
import wres.statistics.generated.Consumer.Format;

/**
 * Utility class for working with statistics messages.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessageUtilities
{

    /**
     * Uncovers a set of declared formats from a description of the outputs.
     * 
     * @param outputs the outputs that declare the formats to write
     * @return the declared formats to write
     */

    public static Set<Format> getDeclaredFormats( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        Set<Format> formats = new HashSet<>();

        if ( outputs.hasPng() )
        {
            formats.add( Format.PNG );
        }

        if ( outputs.hasSvg() )
        {
            formats.add( Format.SVG );
        }

        if ( outputs.hasCsv() )
        {
            formats.add( Format.CSV );
        }

        if ( outputs.hasCsv2() )
        {
            formats.add( Format.CSV2 );
        }

        if ( outputs.hasNetcdf() )
        {
            formats.add( Format.NETCDF );
        }

        if ( outputs.hasProtobuf() )
        {
            formats.add( Format.PROTOBUF );
        }

        return Collections.unmodifiableSet( formats );
    }

    /**
     * Do not construct.
     */

    private MessageUtilities()
    {
    }
}
