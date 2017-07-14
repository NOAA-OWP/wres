package wres.io.config;

import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.io.data.caching.Features;

import java.io.IOException;
import java.util.StringJoiner;

public class ConfigHelper
{
    /**
     * Given a config, generate feature IDs and return a sql string of them.
     *
     * @param config the project config.
     * @return sql string useful in a where clause
     */
    public static String getFeatureIdsAndPutIfAbsent(ProjectConfig config)
    throws IOException
    {
        if (config.getConditions() == null
            || config.getConditions().getFeature() == null)
        {
            return "";
        }

        StringJoiner result = new StringJoiner(",", "feature_id in (", ")");

        try
        {
            // build a sql string of feature_ids, using cache to populate as needed
            for (Conditions.Feature f : config.getConditions().getFeature())
            {
                Integer i = Features.getFeatureID(f.getLid(), f.getName());
                result.add(Integer.toString(i));
            }
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new IOException("Failed to get or put a feature id.", e);
        }

        return result.toString();
    }
}
