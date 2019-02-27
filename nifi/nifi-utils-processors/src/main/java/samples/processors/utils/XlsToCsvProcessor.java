package samples.processors.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import samples.processors.utils.callbacks.XlsToCsvProcessorCallback;

public class XlsToCsvProcessor extends AbstractProcessor {

	public static final PropertyDescriptor SHEET_INDEX = new PropertyDescriptor.Builder()
			.name("SHEET_INDEX")
			.displayName("Sheet Index")
			.description("The index of the sheet to convert")
			.required(true)
			.defaultValue("0")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor SKIP_ROWS = new PropertyDescriptor.Builder()
			.name("SKIP_ROWS")
			.displayName("Skip rows")
			.description("Number of rows to skip before converting to csv")
			.required(false)
			.defaultValue("0")
			.build();
	
	public static final PropertyDescriptor TAKE_ROWS = new PropertyDescriptor.Builder()
			.name("TAKE_ROWS")
			.displayName("Take rows")
			.description("Maximun number of rows to extract from file")
			.required(false)
			.defaultValue("-1")
			.build();
	
	public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
    		.name("File converted")
    		.description("Xls file converted to Csv successfully")
    		.build();
    public static final Relationship ERROR_RELATIONSHIP  = new Relationship.Builder()
    		.name("Failure")
    		.description("Xls file processed with errors")
    		.build();
	
    private Set<Relationship> relationships;
    
    private List<PropertyDescriptor> descriptors;
    
    @Override
    protected void init(ProcessorInitializationContext context) {
    	super.init(context);
    	
    	final ArrayList<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    	descriptors.add(SHEET_INDEX);
    	descriptors.add(SKIP_ROWS);
    	descriptors.add(TAKE_ROWS);
    	this.descriptors = Collections.unmodifiableList(descriptors);
    	
    	final Set<Relationship> relationships = new HashSet<Relationship>();
    	relationships.add(SUCCESS_RELATIONSHIP);
    	relationships.add(ERROR_RELATIONSHIP);
    	this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    public Set<Relationship> getRelationships() {
    	return this.relationships;
    }
    
    @Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}
    
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
            return;
        }
		try {
			int sheetIndex = Integer.parseInt(context.getProperty(SHEET_INDEX).getValue());
			int skipRows = Integer.parseInt(context.getProperty(SKIP_ROWS).getValue());
			int takeRows = Integer.parseInt(context.getProperty(TAKE_ROWS).getValue());
			
			XlsToCsvProcessorCallback processorCallback = new XlsToCsvProcessorCallback(sheetIndex,skipRows,takeRows, getLogger());
			flowFile = session.write(flowFile, processorCallback);
			
			if(processorCallback.SuccessOnProcessing()) {
				session.transfer(flowFile,SUCCESS_RELATIONSHIP);
			}
			else {
				session.transfer(flowFile,ERROR_RELATIONSHIP);
			}
		}
		catch(Exception exc) {
			getLogger().error(exc.getMessage());
		}
	}

}
