package wres.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import javax.xml.bind.JAXBException;

import org.junit.jupiter.api.Test;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;

public class ProjectConfigsTest
{
    /**
     * Tests the {@link ProjectConfigs#compare(wres.config.generated.ProjectConfig, wres.config.generated.ProjectConfig)}.
     */

    private static final String EXAMPLE_DECLARATION =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    + "<project>\n"
    + "    <inputs>\n"
    + "        <left>\n"
    + "            <type>observations</type>\n"
    + "            <source type=\"PI-XML\">FAKE2_observations.xml</source>\n"
    + "            <variable>DISCHARGE</variable>\n"
    + "        </left>\n"
    + "        <right>\n"
    + "             <type>single valued forecasts</type>\n"
    + "             <source type=\"PI-XML\">FAKE2_forecast.xml</source>\n"
    + "             <variable>STREAMFLOW</variable>\n"
    + "        </right>\n"
    + "    </inputs>\n"
    + "    <pair>\n"
    + "        <unit>CMS</unit>\n"
    + "        <feature left=\"FAKE2\" right=\"FAKE2\" />\n"
    + "        <leadHours minimum=\"0\" maximum=\"40\" />\n"
    + "        <issuedDates earliest=\"2551-03-17T00:00:00Z\" latest=\"2551-03-20T00:00:00Z\" />\n"
    + "        <desiredTimeScale>\n"
    + "            <function>mean</function>\n"
    + "            <period>3</period>\n"
    + "            <unit>hours</unit>\n"
    + "        </desiredTimeScale>\n"
    + "        <leadTimesPoolingWindow>\n"
    + "            <period>23</period>\n"
    + "            <frequency>17</frequency>\n"
    + "            <unit>hours</unit>\n"
    + "        </leadTimesPoolingWindow>\n"
    + "    </pair>\n"
    + "    <metrics>\n"
    + "        <metric><name>mean error</name></metric>\n"
    + "        <metric><name>sample size</name></metric>\n"
    + "    </metrics>\n"
    + "    <outputs>\n"
    + "        <destination type=\"csv2\" />\n"
    + "    </outputs>\n"
    + "</project>\n";

    @Test
    public void testCompare()
    {

        // Mock some instances of the project configuration
        // that differ on name
        
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.VOLUMETRIC_EFFICIENCY ) );
        metrics.add( new MetricConfig( null, MetricConfigName.SUM_OF_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );

        ProjectConfig mockOne =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   "MockOne" );

        ProjectConfig mockTwo =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   "MockOne" );

        ProjectConfig mockThree =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   "MockThree" );

        // (x.compareTo(y)==0) == (x.equals(y))
        assertTrue( ( ProjectConfigs.compare( mockOne, mockOne ) == 0 ) == mockOne.equals( mockOne ) );

        assertTrue( ( ProjectConfigs.compare( mockOne, mockTwo ) == 0 ) == mockOne.equals( mockTwo ) );

        assertTrue( ( ProjectConfigs.compare( mockOne, mockThree ) == 0 ) == mockOne.equals( mockThree ) );

        assertTrue( mockOne.equals( mockTwo ) && ProjectConfigs.compare( mockOne, mockTwo ) == 0 );

        // sgn(x.compareTo(y)) == -sgn(y.compareTo(x))
        assertTrue( ProjectConfigs.compare( mockOne, mockOne ) == -ProjectConfigs.compare( mockOne, mockOne ) );

        assertTrue( ProjectConfigs.compare( mockOne, mockTwo ) == -ProjectConfigs.compare( mockTwo, mockOne ) );

        assertTrue( ProjectConfigs.compare( mockOne, mockThree ) == -ProjectConfigs.compare( mockThree, mockOne ) );

        // (x.compareTo(y)>0 && y.compareTo(z)>0) implies x.compareTo(z)>0
        assertTrue( ProjectConfigs.compare( mockThree, mockOne ) > 0 && ProjectConfigs.compare( mockThree, mockTwo ) > 0
                    && ProjectConfigs.compare( mockThree, mockTwo ) > 0 );

        // x.compareTo(y)==0 implies that sgn(x.compareTo(z)) == sgn(y.compareTo(z)), for all z.
        assertTrue( ProjectConfigs.compare( mockOne, mockTwo ) == 0
                    && ProjectConfigs.compare( mockOne, mockThree ) == ProjectConfigs.compare( mockTwo, mockThree ) );

        // Order in a container
        List<ProjectConfig> listOfConfigs = Arrays.asList( mockThree, mockTwo, mockOne );
        Collections.sort( listOfConfigs, Comparator.naturalOrder() );

        assertTrue( listOfConfigs.get( 0 ).equals( mockOne ) && listOfConfigs.get( 1 ).equals( mockTwo )
                    && listOfConfigs.get( 2 ).equals( mockThree ) );
        
    }
    
    /**
     * Tests for an expected exception from {@link ProjectConfigs#compare(wres.config.generated.ProjectConfig, 
     * wres.config.generated.ProjectConfig)} when the first input is null.
     */

    @Test
    public void testCompareThrowsNullWhenFirstInputIsNull()
    {
        // Nullity with null input
        assertThrows( NullPointerException.class,
                      () -> ProjectConfigs.compare( null,
                                                    new ProjectConfig( null, null, null, null, null, null ) ) );
    }
    
    /**
     * Tests for an expected exception from {@link ProjectConfigs#compare(wres.config.generated.ProjectConfig, 
     * wres.config.generated.ProjectConfig)} when the second input is null.
     */

    @Test
    public void testCompareThrowsNullWhenSecondInputIsNull()
    {
        // Nullity with null input
        assertThrows( NullPointerException.class,
                      () -> ProjectConfigs.compare( new ProjectConfig( null, null, null, null, null, null ),
                                                    null ) );
    }

    @Test
    void sourceGetsAddedToDeclarationWithOtherExistingSources()
            throws IOException, JAXBException
    {
        String originalDeclaration = EXAMPLE_DECLARATION;
        Random random = new Random( System.currentTimeMillis() );
        InterfaceShortHand newInterface = InterfaceShortHand.USGS_NWIS;
        String newSource = "https://test/?something=" + random.nextLong();
        URI uri = URI.create( newSource );

        DataSourceConfig.Source toAdd =
                new DataSourceConfig.Source( uri,
                                             newInterface,
                                             null,
                                             null,
                                             null,
                                             null );
        String resultDeclaration = ProjectConfigs.addSource( originalDeclaration,
                                                             "a test class",
                                                             LeftOrRightOrBaseline.LEFT,
                                                             toAdd );
        assertFalse( originalDeclaration.contains( newSource ),
                     "The original should not have the new source but did:\n"
                     + originalDeclaration );
        assertTrue( resultDeclaration.contains( newSource ),
                    "The result should have the new source but did not:\n"
                    + resultDeclaration );
    }
}
