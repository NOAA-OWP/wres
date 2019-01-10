package wres.systests;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Records a single line from the pairs file.
 * @author <a href="mailto:Raymond.Chui@***REMOVED***">
 * @version 1.5
 * Created: December, 2018
 */
public class PairsFilePair
{
    //TODO I don't think this is required anymore.  With the latest system test code, the pairs file is not interpreted, 
    //just sorted and compared with each line treated as a long string.  May want to remove this class when ready. 
    //I leave it hear for now.
    
   
    private String siteID; // 1
    private String earlestIssueTime; // 2
    private String latestIssueTime; // 3
    private String earlestValidTime; // 4
    private String latestValieTime; // 5
    private Integer earlestLeadTimeInSeconds; // 6
    private Integer latestLeadTimeInSeconds; // 7
    private String validTimePairs; // 8
    private Integer leadDurationPairInSeconds; // 9
    private ArrayList<String> leftValueAndRightMembers; // the rest

    /**
     * @param pairsLine  The line from which to draw the data.  
     */
    public PairsFilePair( String pairsLine )
    {
        String[] delimiter = pairsLine.split( "," );
        ArrayList<String> list = new ArrayList<String>();

        for ( int i = 9; i < delimiter.length; i++ )
        {
            list.add( delimiter[i] );
        }

        initialize(
                    delimiter[0], // siteID
                    delimiter[1], // earlestIssueTime
                    delimiter[2], // latestIssueTime
                    delimiter[3], // earlestValidTime
                    delimiter[4], // latestValieTime
                    Integer.parseInt( delimiter[5] ), // earlestLeadTimeInSeconds
                    Integer.parseInt( delimiter[6] ), // latestLeadTimeInSeconds
                    delimiter[7], // validTimePairs
                    Integer.parseInt( delimiter[8] ), // leadDurationPairInSeconds
                    list // leftAndrightMemberInCMS
        );
    }

    public PairsFilePair(
                          String siteID,
                          String earlestIssueTime,
                          String latestIssueTime,
                          String earlestValidTime,
                          String latestValieTime,
                          Integer earlestLeadTimeInSeconds,
                          Integer latestLeadTimeInSeconds,
                          String validTimePairs,
                          Integer leadDurationPairInSeconds,
                          ArrayList<String> leftAndrightMemberInCMS )
    {
        this.initialize( siteID,
                         earlestIssueTime,
                         latestIssueTime,
                         earlestValidTime,
                         latestValieTime,
                         earlestLeadTimeInSeconds,
                         latestLeadTimeInSeconds,
                         validTimePairs,
                         leadDurationPairInSeconds,
                         leftAndrightMemberInCMS );
    }

    private void initialize(
                             String siteID,
                             String earlestIssueTime,
                             String latestIssueTime,
                             String earlestValidTime,
                             String latestValieTime,
                             Integer earlestLeadTimeInSeconds,
                             Integer latestLeadTimeInSeconds,
                             String validTimePairs,
                             Integer leadDurationPairInSeconds,
                             ArrayList<String> leftAndrightMemberInCMS )
    {
        this.siteID = siteID;
        this.earlestIssueTime = earlestIssueTime;
        this.latestIssueTime = latestIssueTime;
        this.earlestValidTime = earlestValidTime;
        this.latestValieTime = latestValieTime;
        this.earlestLeadTimeInSeconds = earlestLeadTimeInSeconds;
        this.latestLeadTimeInSeconds = latestLeadTimeInSeconds;
        this.validTimePairs = validTimePairs;
        this.leadDurationPairInSeconds = leadDurationPairInSeconds;
        this.leftValueAndRightMembers = leftAndrightMemberInCMS;
    }

    public void writeToPairsFile( BufferedWriter bufferedWriter ) throws IOException
    {
        bufferedWriter.write(
                              getSiteID() + ","
                              +
                              getEarlestIssueTime()
                              + ","
                              +
                              getLatestIssueTime()
                              + ","
                              +
                              getEarlestValidTime()
                              + ","
                              +
                              getLatestValieTime()
                              + ","
                              +
                              getEarlestLeadTimeInSeconds().intValue()
                              + ","
                              +
                              getLatestLeadTimeInSeconds().intValue()
                              + ","
                              +
                              getValidTimePairs()
                              + ","
                              +
                              getLeadDurationPairInSeconds().intValue() );

        for ( Iterator<String> iterator2 =
                getLeftValuesAndRightMembers().iterator(); iterator2.hasNext(); )
        {
            bufferedWriter.write( "," +
                                  iterator2.next().toString() );
        }
        bufferedWriter.newLine();
    }

    public String getSiteID()
    {
        return this.siteID;
    }

    public String getEarlestIssueTime()
    {
        return this.earlestIssueTime;
    }

    public String getLatestIssueTime()
    {
        return this.latestIssueTime;
    }

    public String getEarlestValidTime()
    {
        return this.earlestValidTime;
    }

    public String getLatestValieTime()
    {
        return this.latestValieTime;
    }

    public Integer getEarlestLeadTimeInSeconds()
    {
        return this.earlestLeadTimeInSeconds;
    }

    public Integer getLatestLeadTimeInSeconds()
    {
        return this.latestLeadTimeInSeconds;
    }

    public String getValidTimePairs()
    {
        return this.validTimePairs;
    }

    public Integer getLeadDurationPairInSeconds()
    {
        return this.leadDurationPairInSeconds;
    }

    public ArrayList<String> getLeftValuesAndRightMembers()
    {
        return this.leftValueAndRightMembers;
    }
}
