import java.util.*;

/**
 * The pairs class with these data
 * @author <a href="mailto:Raymond.Chui@***REMOVED***">
 * @version 1.5
 * Created: December, 2018
 */
public class ThePairs {
	private String siteID; // 1
	private String earlestIssueTime; // 2
	private String latestIssueTime; // 3
	private String earlestValidTime; // 4
	private String latestValieTime; // 5
	private Integer earlestLeadTimeInSeconds; // 6
	private Integer latestLeadTimeInSeconds; // 7
	private String validTimePairs; // 8
	private Integer leadDurationPairInSeconds; // 9
	private ArrayList<String> leftAndrightMemberInCMS; // the rest

    public ThePairs(
                    String siteID,
					String earlestIssueTime,
					String latestIssueTime,
					String earlestValidTime,
					String latestValieTime,
					Integer earlestLeadTimeInSeconds,
					Integer latestLeadTimeInSeconds,
					String validTimePairs,
					Integer leadDurationPairInSeconds,
					ArrayList<String> leftAndrightMemberInCMS
            ) {

        this.siteID = siteID;
		this.earlestIssueTime = earlestIssueTime;
		this.latestIssueTime = latestIssueTime;
		this.earlestValidTime = earlestValidTime;
		this.latestValieTime = latestValieTime;
		this.earlestLeadTimeInSeconds = earlestLeadTimeInSeconds;
		this.latestLeadTimeInSeconds = latestLeadTimeInSeconds;
		this.validTimePairs = validTimePairs;
		this.leadDurationPairInSeconds = leadDurationPairInSeconds;
		this.leftAndrightMemberInCMS = leftAndrightMemberInCMS;
    } // end this construction

    public String getSiteID() { return this.siteID; }
    public String getEarlestIssueTime() { return this.earlestIssueTime; }
	public String getLatestIssueTime() { return this.latestIssueTime; }
	public String getEarlestValidTime() { return this.earlestValidTime; }
	public String getLatestValieTime() { return this.latestValieTime; }
	public Integer getEarlestLeadTimeInSeconds() { return this.earlestLeadTimeInSeconds; }
	public Integer getLatestLeadTimeInSeconds() { return this.latestLeadTimeInSeconds; }
	public String getValidTimePairs() { return this.validTimePairs; }
	public Integer getLeadDurationPairInSeconds() { return this.leadDurationPairInSeconds; }
	public ArrayList<String> getLeftAndRightMemberInCMS() { return this.leftAndrightMemberInCMS; }	
} // end ThePairs class
