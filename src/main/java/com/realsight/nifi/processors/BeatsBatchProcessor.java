package com.realsight.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

@SideEffectFree
@Tags({"QUEUE", "NIFI Batch"})
@CapabilityDescription("batch processing")
public class BeatsBatchProcessor extends AbstractProcessor {
	
	 private List<PropertyDescriptor> properties;
	 private Set<Relationship> relationships;
	 
	 private Gson gson;
	 
	 private static  int MAX_SIZE = 30;
	 
	 private static  int TIME_OUT = 60*1000;// 1 min
	 
	 private final Logger logger = LoggerFactory.getLogger(BeatsBatchProcessor.class);
	 
	public static final PropertyDescriptor MAX_SIZE_PD = new PropertyDescriptor.Builder()
			.name("MAX SIZE")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.description("max size of queue")
			.defaultValue("100")
			.build();
	
	public static final PropertyDescriptor TIME_OUT_PD = new PropertyDescriptor.Builder()
			.name("TIME OUT")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("60000")
			.description("ms")
			.build();
	
    public static final  Relationship SUCCESS = new  Relationship.Builder()
    		.name("SUCCESS")
    		.description("success relationship")
    		.build();
    
	private AtomicReference<ConcurrentLinkedQueue<String>> cacheQueue = 
			new AtomicReference<ConcurrentLinkedQueue<String>>();
	
	private AtomicLong stratTimestamp = new AtomicLong(System.currentTimeMillis());
	
	@Override
	public void init(final ProcessorInitializationContext context){
	
		List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MAX_SIZE_PD);
        properties.add(TIME_OUT_PD);
        
        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
       // relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        stratTimestamp.set(System.currentTimeMillis());
        
        cacheQueue.set(new ConcurrentLinkedQueue<String>());
        
        gson = new Gson();
       logger.debug(BeatsBatchProcessor.class.getName()+"  init success");
	}
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		AtomicReference<String> value = new AtomicReference<String>();
		ConcurrentLinkedQueue<String> queue = cacheQueue.get();
		
		 FlowFile flowfile = session.get();
		 session.read(flowfile, new InputStreamCallback(){
			 @Override
	            public void process(InputStream in) throws IOException {
	              String  jsonStr = IOUtils.toString(in, Charsets.UTF_8);
	              value.set(jsonStr);
	            }
		 });
		 
		 queue.add(value.get());
		 
		String maxSizeStr =  context.getProperty(MAX_SIZE_PD).evaluateAttributeExpressions().getValue();
		int maxSize = getInteger(maxSizeStr,MAX_SIZE);
		
		String timeOutStr = context.getProperty(TIME_OUT_PD).evaluateAttributeExpressions().getValue();
		
		int timeOut = getInteger(timeOutStr,TIME_OUT);
		
		if(queue.size() >= maxSize || timeOut <=(System.currentTimeMillis() -  stratTimestamp.get())){
			ConcurrentLinkedQueue<String>	newQueue = new  ConcurrentLinkedQueue<String>();
			if(cacheQueue.compareAndSet(queue, newQueue)){
				String message = gson.toJson(queue);
			   flowfile=session.write(flowfile, new OutputStreamCallback(){
				@Override
				public void process(OutputStream out) throws IOException {
					out.write(message.getBytes());
				}  
			   });
			 session.transfer(flowfile,SUCCESS);
			}
			else
				session.remove(flowfile);
		}
		else{
			session.remove(flowfile);
		}
		
	}

	
	@Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
    
    private int getInteger(String valueStr,int defaultValue){
	    	int valueInt;
	    	if(valueStr.matches("^[0-9]*$"))
			 {
	    			valueInt = Integer.valueOf(valueStr);
				if(valueInt <=  0){
					valueInt = defaultValue;
				}
				return valueInt;
			 }
	    	else
	    		return defaultValue;
    }
    
  /*
   
   @Test
    public static void testConnectionFailure(){
    			final TestRunner runner = 
    					TestRunners.newTestRunner(new BeatsBatchProcessor());
    			//runner.setProperty(BeatsBatchProcessor.SUCCESS, "ABCD");
    			runner.setProperty(BeatsBatchProcessor.MAX_SIZE_PD, "abc");
    			runner.setProperty(TIME_OUT_PD, "tttt");
    			runner.setValidateExpressionUsage(false);
    		//	runner.assertAllFlowFilesTransferred(BeatsBatchProcessor.SUCCESS);
    			//runner.assertTransferCount(SUCCESS, 2);
    			for(int i=0;i< BeatsBatchProcessor.MAX_SIZE*3;i++)
    			{
    				
    				byte[] content = "abc".getBytes();
    				runner.enqueue(content);
    				runner.run();
    			}
    		
    			List<MockFlowFile> list= runner.getFlowFilesForRelationship(SUCCESS);
    			
    			for(MockFlowFile mff : list){
    				System.out.println(new String(mff.toByteArray()));
    			}
    			 
    }
    
    public static void main(String[] arg0){
    		System.out.println("start ……");
    		testConnectionFailure();
    		
    }
    */
}
