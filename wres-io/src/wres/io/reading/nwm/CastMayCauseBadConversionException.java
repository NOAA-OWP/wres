package wres.io.reading.nwm;

/**
 * Thrown when cast may cause bad conversion due to NWM netCDF data structures.
 *<p>
 * For example, if a 32-bit float approximating 0.01 is in an attribute and is
 * to be multiplied by a 32-bit integer, even though promotion of a 32-bit
 * float to double can cause no data loss strictly speaking, a different result
 * would occur for a 64-bit double approximating 0.01 (it's a different number),
 * and there are times when a 32-bit float times a 32-bit integer will result
 * in an exact representation of the original data versus when promoting from
 * 32-bit float to 64-bit double prior to the unpacking. See issue #77779
 * </p>
 */
class CastMayCauseBadConversionException extends RuntimeException
{
    CastMayCauseBadConversionException( String message )
    {
        super( message );
    }
}
