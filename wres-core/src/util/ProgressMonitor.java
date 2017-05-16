/**
 * 
 */
package util;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.function.BiConsumer;

/**
 * @author Christopher Tubbs
 *
 */
public class ProgressMonitor
{    
    public ProgressMonitor(PrintStream printer) {
        this.totalSteps = 0;
        this.completedSteps = 0;
        this.formatter = new DecimalFormat();
        this.formatter.setMaximumFractionDigits(2);
        
        if (printer == null)
        {
            printer = System.out;
        }
        
        this.printer = printer;
        this.outputFunction = (Integer completed, Integer total) -> {
            this.outputProgress();
        };
    }
    
    public ProgressMonitor()
    {
        this.totalSteps = 0;
        this.completedSteps = 0;
        this.formatter = new DecimalFormat();
        this.formatter.setMaximumFractionDigits(2);
        this.printer = System.out;
        this.outputFunction = (Integer completed, Integer total) -> {
            this.outputProgress();
        };
    }
    
    public ProgressMonitor(BiConsumer<Integer, Integer> outputFunction)
    {
        this.totalSteps = 0;
        this.completedSteps = 0;
        this.formatter = new DecimalFormat();
        this.formatter.setMaximumFractionDigits(2);
        this.printer = System.out;
        this.outputFunction = outputFunction;
    }
    
    public void UpdateMonitor() {
        
        if (completedSteps < totalSteps) {
            completedSteps++;
            this.outputFunction.accept(completedSteps, totalSteps);
        }
        
        if (this.autoReset && totalSteps.equals(completedSteps)) {
            reset();
        }
    }
    
    private void outputProgress() {
        this.printer.print(getProgressMessage());
    }
    
    public void addStep() {
            this.totalSteps++;
            this.outputFunction.accept(completedSteps, totalSteps);
    }
    
    public void setSteps(int steps) {
            this.totalSteps = steps;
            this.outputFunction.accept(completedSteps, totalSteps);
    }
    
    public void setAutoReset(boolean reset) {
        this.autoReset = reset;
    }
    
    public void setShowPercentage(boolean showPercentage) {
        this.showPercentage = showPercentage;
    }
    
    public void setShowStepDescription(boolean showStepDescription)
    {
        this.showStepDescription = showStepDescription;
    }
    
    public void reset() {
        this.totalSteps = 0;
        this.completedSteps = 0;
        this.printer.println();
    }
    
    private String getProgressMessage() {
        String message = "COULDN'T CREATE UPDATE MESSAGE";
        String builder = "\r";
        try
        {
            if (this.showStepDescription)
            {
                builder += completedSteps;
                builder += " steps out of ";
                builder += totalSteps;
                builder += " completed";
            }
            
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
    
    public void changePrinter(PrintStream printer)
    {
        this.printer = printer;
    }

    private Integer totalSteps;
    private Integer completedSteps;
    private boolean autoReset;
    private boolean showPercentage = false;
    private boolean showStepDescription = true;
    private final DecimalFormat formatter;
    private PrintStream printer;
    private BiConsumer<Integer, Integer> outputFunction;
}
