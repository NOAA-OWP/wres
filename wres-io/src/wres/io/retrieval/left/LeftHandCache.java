package wres.io.retrieval.left;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collection;

import wres.config.generated.Feature;
import wres.io.project.Project;
import wres.io.utilities.NoDataException;

public interface LeftHandCache
{

    /**
     * @param project the project
     * @param feature the feature
     * @return the left hand cache
     * @throws NoDataException when no data is found
     * @throws SQLException if data could not be retrieved from the database
     */

    static LeftHandCache getCache( final Project project, final Feature feature)
            throws SQLException
    {
        LeftHandCache cache;

        if ( project.usesGriddedData( project.getLeft() ))
        {
            cache = new GridCache( project );
        }
        else
        {
            cache = new VectorCache( project, feature );
        }

        return cache;
    }

    Collection<Double> getLeftValues(
            final Feature feature,
            final LocalDateTime earliestDate,
            final LocalDateTime latestDateTime)
            throws IOException;
}
