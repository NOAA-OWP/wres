package wres.control;

/**
     * An exception representing that execution of a step failed.
     * Needed because Java 8 Function world does not
     * deal kindly with checked Exceptions.
     */
    class WresProcessingException extends RuntimeException
    {
        private static final long serialVersionUID = 6988169716259295343L;

        WresProcessingException( String message )
        {
            super( message );
        }

        WresProcessingException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }
