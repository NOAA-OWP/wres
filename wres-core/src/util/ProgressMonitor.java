/**
 * 
 */
package util;

import java.text.DecimalFormat;

/**
 * @author Christopher Tubbs
 *
 */
public class ProgressMonitor
{    
    public ProgressMonitor() {
        this.totalSteps = 0;
        this.completedSteps = 0;
        formatter = new DecimalFormat();
        formatter.setMaximumFractionDigits(2);
    }
    
    public void UpdateMonitor() {
        
        if (completedSteps < totalSteps) {
            completedSteps++;
            outputProgress();
        }
        
        if (this.autoReset && totalSteps.equals(completedSteps)) {
            reset();
        }
    }
    
    private void outputProgress() {
        System.out.print(getProgressMessage());
    }
    
    public void addStep() {
            this.totalSteps++;
            outputProgress();
    }
    
    public void setSteps(int steps) {
            this.totalSteps = steps;
            outputProgress();
    }
    
    public void setAutoReset(boolean reset) {
        this.autoReset = reset;
    }
    
    public void setShowPercentage(boolean showPercentage) {
        this.showPercentage = showPercentage;
    }
    
    public void reset() {
        this.totalSteps = 0;
        this.completedSteps = 0;
        System.out.println();
        System.out.println();
    }
    
    private String getProgressMessage() {
        String message = "COULDN'T CREATE UPDATE MESSAGE";
        String builder = "\r";
        try
        {
            builder += completedSteps;
            builder += " steps out of ";
            builder += totalSteps;
            builder += " completed";
            if (this.showPercentage) {
                builder += " (";
                builder += getCompletionPercent();
                builder += "%)";
            }
            
            builder += "...";
            message = builder;
        }
        catch (Exception e) {
            System.err.println();
            builder = builder.replaceAll("\\", "\\\\");
            System.err.println("Could not print: ");
            System.err.println(builder);
            e.printStackTrace();
        }
        return message;
    }
    
    private String getCompletionPercent() {
        return formatter.format(((completedSteps * 1.0)/(totalSteps * 1.0))*100);
    }

    private Integer totalSteps;
    private Integer completedSteps;
    private boolean autoReset;
    private boolean showPercentage = true;
    private final DecimalFormat formatter;
}
