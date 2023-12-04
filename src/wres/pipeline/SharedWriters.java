package wres.pipeline;


import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import wres.io.writing.SharedSampleDataWriters;

/**
 * A value object for shared writers.
 * @param sharedSampleWriters Shared writers for sample data.
 * @param sharedBaselineSampleWriters Shared writers for baseline sampled data.
 */

record SharedWriters( SharedSampleDataWriters sharedSampleWriters,
                      SharedSampleDataWriters sharedBaselineSampleWriters ) implements Closeable
{
    /**
     * Returns an instance.
     *
     * @param sharedSampleWriters shared writer of pairs
     * @param sharedBaselineSampleWriters shared writer of baseline pairs
     */
    static SharedWriters of( SharedSampleDataWriters sharedSampleWriters,
                             SharedSampleDataWriters sharedBaselineSampleWriters )

    {
        return new SharedWriters( sharedSampleWriters, sharedBaselineSampleWriters );
    }


    /**
     * Attempts to close all shared writers.
     * @throws IOException when a resource could not be closed
     */

    @Override
    public void close() throws IOException
    {
        if ( this.hasSharedSampleWriters() )
        {
            this.getSampleDataWriters().close();
        }

        if ( this.hasSharedBaselineSampleWriters() )
        {
            this.getBaselineSampleDataWriters().close();
        }
    }

    /**
     * Returns the shared sample data writers.
     *
     * @return the shared sample data writers.
     */

    SharedSampleDataWriters getSampleDataWriters()
    {
        return this.sharedSampleWriters;
    }

    /**
     * Returns the shared sample data writers for baseline data.
     *
     * @return the shared sample data writers  for baseline data.
     */

    SharedSampleDataWriters getBaselineSampleDataWriters()
    {
        return this.sharedBaselineSampleWriters;
    }

    /**
     * Returns <code>true</code> if shared sample writers are available, otherwise <code>false</code>.
     *
     * @return true if shared sample writers are available
     */

    boolean hasSharedSampleWriters()
    {
        return Objects.nonNull( this.sharedSampleWriters );
    }

    /**
     * Returns <code>true</code> if shared sample writers are available for the baseline samples, otherwise
     * <code>false</code>.
     *
     * @return true if shared sample writers are available for the baseline samples
     */

    boolean hasSharedBaselineSampleWriters()
    {
        return Objects.nonNull( this.sharedBaselineSampleWriters );
    }
}
