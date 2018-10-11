package wres.tasker;

/**
 * Use custom RuntimeException to notify when a parse error occurs.
 *
 * Parse error could mean we messed up somewhere in versions of a dependency,
 * looked in the wrong exchange on the broker, or looked at the wrong topic on
 * the broker, or messed up one of our protobuf messages on either side.
 */

class WresParseException extends RuntimeException
{
    public WresParseException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public WresParseException( Throwable cause )
    {
        super( cause );
    }

    public WresParseException( String message )
    {
        super( message );
    }
}
