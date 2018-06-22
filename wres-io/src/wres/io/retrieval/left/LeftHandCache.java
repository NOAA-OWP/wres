package wres.io.retrieval.left;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;

public interface LeftHandCache
{
    static LeftHandCache getCache(final ProjectDetails projectDetails, final Feature feature)
            throws SQLException, NoDataException
    {
        LeftHandCache cache;

        if (projectDetails.usesGriddedData( projectDetails.getLeft() ))
        {
            cache = new GridCache( projectDetails );
        }
        else
        {
            cache = new VectorCache( projectDetails, feature );
        }

        return cache;
    }

    List<Double> getLeftValues(
            final Feature feature,
            final LocalDateTime earliestDate,
            final LocalDateTime latestDateTime)
            throws IOException;
}
