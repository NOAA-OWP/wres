package wres.io.config;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.io.data.caching.Features;
import wres.io.utilities.Database;

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
    throws Exception // Needed due to signature of powermock doReturn().when()
    {
        // Need to intercept a call in Features.class
        PowerMockito.spy(Features.class);

        // anyString() will return FALSE if the arg is null... need to use any() matcher!!
        doReturn(1, 2,3).when(Features.class, "getFeatureID", any());

        String expected = "feature_id in (1,2,3)";

        Feature featureFakeOne = new Feature( null, null, null, "fake1", null, null, null, null, null, null );
        Feature featureFakeTwo = new Feature(null, null, null, "fake2", null, null, null, null, null, null );
        Feature featureFakeThree = new Feature(null, null, null, "fake3", null, null, null, null, null, null );

        List<Feature> features = new ArrayList<>();
        features.add(featureFakeOne);
        features.add(featureFakeTwo);
        features.add(featureFakeThree);

        PairConfig c = new PairConfig( null, features, null, null, null, null, null, null, null, null, null );
        ProjectConfig config = new ProjectConfig(null, c, null, null, null, "test");
        String result = ConfigHelper.getFeatureIdsAndPutIfAbsent(config);
        assertEquals(expected, result);
    }
}
