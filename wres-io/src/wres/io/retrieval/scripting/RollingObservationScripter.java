package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.NotImplementedException;

class RollingObservationScripter extends Scripter
{
    protected RollingObservationScripter( ProjectDetails projectDetails,
                                          DataSourceConfig dataSourceConfig,
                                          Feature feature,
                                          int progress,
                                          int sequenceStep)
    {
        super( projectDetails, dataSourceConfig, feature, progress, sequenceStep );
    }

    @Override
    String formScript() throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        throw new NotImplementedException( "Scripts for loading rolling observations have not been written yet." );
        //return null;
    }

    @Override
    String getBaseDateName()
    {
        return null;
    }

    @Override
    String getValueDate()
    {
        return null;
    }
}
