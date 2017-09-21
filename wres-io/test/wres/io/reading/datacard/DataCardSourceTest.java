package wres.io.reading.datacard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DataSourceConfig.Source;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeZone;
import wres.io.config.ConfigHelper;
import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.util.FormattedStopwatch;

public class DataCardSourceTest
{
	
	private DatacardSource source = null;
	private static final String EXPECTED_QUERY_NORMAL = "123|'1960-01-01T19:00Z'|201.0|456|789\n" + 
														 "123|'1960-01-02T19:00Z'|181.0|456|789\n" + 
														 "123|'1960-01-03T19:00Z'|243.0|456|789\n" + 
														 "123|'1960-01-04T19:00Z'|409.0|456|789\n" + 
														 "123|'1960-01-05T19:00Z'|364.0|456|789\n" + 
														 "123|'1960-01-06T19:00Z'|306.0|456|789\n" + 
														 "123|'1960-01-07T19:00Z'|268.0|456|789\n" + 
														 "123|'1960-01-08T19:00Z'|240.0|456|789\n" + 
														 "123|'1960-01-09T19:00Z'|220.0|456|789\n" + 
														 "123|'1960-01-10T19:00Z'|198.0|456|789";
	
	private static final String EXPECTED_QUERY_SHORT_RECORD = "123|'1949-01-01T01:00Z'|0.65|456|789\n" + 
															"123|'1949-01-01T07:00Z'|0.229|456|789\n" + 
															"123|'1949-01-01T19:00Z'|0.024|456|789\n" + 
															"123|'1949-01-02T01:00Z'|0.025|456|789\n" + 
															"123|'1949-01-02T07:00Z'|0.001|456|789\n" + 
															"123|'1949-01-03T19:00Z'|0.001|456|789\n" + 
															"123|'1949-01-04T07:00Z'|0.015|456|789\n" + 
															"123|'1949-01-04T19:00Z'|0.028|456|789\n" + 
															"123|'1949-01-05T01:00Z'|0.526|456|789\n" + 
															"123|'1949-01-05T07:00Z'|0.263|456|789\n" + 
															"123|'1949-01-05T13:00Z'|0.188|456|789\n" + 
															"123|'1949-01-05T19:00Z'|0.022|456|789";

	@Test
    public void insertQueryNormalTest()
    {
		try
        {
        	String current = new java.io.File( "." ).getCanonicalPath();
        	List<DataSourceConfig.Source> sourceList = new ArrayList<DataSourceConfig.Source>();
        	Format f = Format.fromValue("datacard");
        	
        	DataSourceConfig.Source confSource = new Source(current, f, "IN", "DRRC2", 
        													null, TimeZone.fromValue("EST"), "-999.0", true,
														    true, "-998.0", "$");
        	
        	sourceList.add(confSource);
        	
        	DataSourceConfig config = new DataSourceConfig(DatasourceType.fromValue("observations"), 
    													   sourceList, 
    													   new Variable("QINE", "test", "IN"), 
    													   null, 
    													   null, 
    													   null, 
    													   null, 
    													   null, 
    													   null);
        		
    		source = new wres.io.reading.datacard.DatacardSource(current + "/testinput/datacard/short_HOPR1SNE.QME.OBS");
    		source.setDataSourceConfig(config);
    		source.setTestMode(true);
    		
    		source.setVariablePositionID(123);
    		source.setMeasurementID(456);
    		source.setSourceID(789);
           	source.saveObservation();
        	
        	String query = source.getInsertQuery();
        	assertTrue("Expected equal outputs.", query.equals(EXPECTED_QUERY_NORMAL));
        }
        catch(IOException e)
        {
        	
        }
	}
	
	@Test
	//Test short record and multiple specified missing values
    public void insertQueryShortRecordTest()
    {
		try
        {
        	String current = new java.io.File( "." ).getCanonicalPath();
        	List<DataSourceConfig.Source> sourceList = new ArrayList<DataSourceConfig.Source>();
        	Format f = Format.fromValue("datacard");
        	
        	DataSourceConfig.Source confSource = new Source(current, f, "IN", "DRRC2", 
        													null, TimeZone.fromValue("EST"), "-999.0, -997", true,
														    true, "-998.0", "$");
        	
        	sourceList.add(confSource);
        	
        	DataSourceConfig config = new DataSourceConfig(DatasourceType.fromValue("observations"), 
    													   sourceList, 
    													   new Variable("QINE", "test", "IN"), 
    													   null, 
    													   null, 
    													   null, 
    													   null, 
    													   null, 
    													   null);
        		
    		source = new wres.io.reading.datacard.DatacardSource(current + "/testinput/datacard/short_CCRN6.MAP06_short_record");
    		source.setDataSourceConfig(config);
    		source.setTestMode(true);
    		
    		source.setVariablePositionID(123);
    		source.setMeasurementID(456);
    		source.setSourceID(789);
           	source.saveObservation();
        	
        	String query = source.getInsertQuery();
        	assertTrue("Expected equal outputs.", query.equals(EXPECTED_QUERY_SHORT_RECORD));
        }
        catch(IOException e)
        {
        	
        }
	}

}
