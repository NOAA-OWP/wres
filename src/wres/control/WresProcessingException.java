package wres.control;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
     * An exception representing that execution of a step failed.
     * Needed because Java 8 Function world does not
     * deal kindly with checked Exceptions.
     */
    class WresProcessingException extends RuntimeException
    {
        private static final long serialVersionUID = 6988169716259295343L;
        private static final Object EXCEPTION_LOCK = new Object();

        private static List<WresProcessingException> occurrences;

        public static void addOccurrence(WresProcessingException recent)
        {
            synchronized ( EXCEPTION_LOCK )
            {
                if (occurrences == null)
                {
                    occurrences = new ArrayList<>(  );
                }

                WresProcessingException.occurrences.add( recent );
            }
        }

        public static List<WresProcessingException> getOccurrences()
        {
            synchronized ( EXCEPTION_LOCK )
            {
                if (occurrences == null)
                {
                    occurrences = new ArrayList<>(  );
                }

                return Collections.unmodifiableList(occurrences);
            }
        }

        WresProcessingException( String message )
        {
            super( message );
            WresProcessingException.addOccurrence( this );
        }
        
        WresProcessingException( String message, Throwable cause )
        {
            super( message, cause );
            WresProcessingException.addOccurrence( this );
        }
    }
