package wres.io.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import wres.config.generated.Conditions;
import wres.config.generated.Location;
import wres.config.generated.ProjectConfig;
import wres.io.data.caching.Features;
import wres.io.utilities.Database;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.doReturn;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SystemSettings.class, Database.class, Features.class})//, Cache.class})
@PowerMockIgnore("javax.management.*")
public class ConfigHelperTest
{
    private ComboPooledDataSource comboPooledDataSource;

    @Before
    public void setup() throws IOException, SQLException
    {
        comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl("null/null/null");
        // Nerf SystemSettings
        PowerMockito.mockStatic(SystemSettings.class);
        PowerMockito.when(SystemSettings.getConnectionPool()).thenReturn(comboPooledDataSource);
        // Nerf Database
        PowerMockito.mockStatic(Database.class);
        PowerMockito.when(Database.getPool()).thenReturn(comboPooledDataSource);

    }

    @Test
    public void featureIdsTest()
    throws IOException, SQLException
    {
        // Need to intercept a call in Features.class
        PowerMockito.spy(Features.class);
        try
        {
            // anyString() will return FALSE if the arg is null... need to use any() matcher!!
            doReturn(1, 2,3).when(Features.class, "getFeatureID", any(), any());
        }
        catch (Exception e) // TODO: remove when Features.getFeatureID does not throw Exception
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new IOException("Issue in Features.getFeatureID", e);
        }

        String expected = "feature_id in (1,2,3)";

        Conditions.Feature featureFakeOne = new Conditions.Feature(null, null, new Location("fake1", null, null, null, null), null, null, false);
        Conditions.Feature featureFakeTwo = new Conditions.Feature(null, null,  new Location("fake2", null, null, null, null), null, null, false);
        Conditions.Feature featureFakeThree = new Conditions.Feature(null, null,  new Location("fake3", null, null, null, null), null, null, false);

        List<Conditions.Feature> features = new ArrayList<>();
        features.add(featureFakeOne);
        features.add(featureFakeTwo);
        features.add(featureFakeThree);

        Conditions c = new Conditions(null, null, null, features, null, 1, 2818644);
        ProjectConfig config = new ProjectConfig(null, c, null, null, null, null);
        String result = ConfigHelper.getFeatureIdsAndPutIfAbsent(config);
        assertEquals(expected, result);
    }
}
