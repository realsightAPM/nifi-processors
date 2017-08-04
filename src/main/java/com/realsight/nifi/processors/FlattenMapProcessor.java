/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.realsight.nifi.processors;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author phillip
 */
@SideEffectFree
@Tags({"JSON", "NIFI ROCKS"})
@CapabilityDescription("Fetch value from json path.")
public class FlattenMapProcessor extends AbstractProcessor {
   
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final Logger logger = LoggerFactory.getLogger(FlattenMapProcessor.class);

    private Gson gson;
    private final AtomicReference<String> value = new AtomicReference<>();
    private List<Map<String, Object>> result = new ArrayList<>();

    public static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("Timestamp field for rs_timestamp")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_DRIVEN = new PropertyDescriptor.Builder()
            .name("Data Driven Schema or Not")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();
    
    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TIMESTAMP_FIELD);
        properties.add(DATA_DRIVEN);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        gson = new Gson();
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        
        //final AtomicReference<String> value = new AtomicReference<>();
        FlowFile flowfile = session.get();
        if(flowfile == null){
            return;
        }
        //logger.error("Size: " + flowfile.getSize());
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                String json = IOUtils.toString(in);
                Map<String, Object> flattenJson = new JsonFlattener(json).withSeparator('_').flattenAsMap();
                Map<String, Object> datas = new HashedMap();
                //List<Map<String, Object>> result = new ArrayList<>();
                String flag = context.getProperty(DATA_DRIVEN).getValue();

                for(Map.Entry<String, Object> entry: flattenJson.entrySet()){
                    String key = entry.getKey();
                    String value = entry.getValue().toString();
                    if(key.equals(context.getProperty(TIMESTAMP_FIELD).getValue())){
                        datas.put("rs_timestamp", value);
                    }
                    else {
                        if(flag.equals("true")){
                            if (StringUtils.isNumeric(value)){
                                datas.put(key + "_f", value);
                            }
                            else {
                                datas.put(key + "_s", value);
                            }
                        }
                        else{
                            datas.put(key, value);
                        }
                    }
                }

                result.add(datas);
                //logger.error("Result: " + gson.toJson(result));
                value.set(gson.toJson(result));
            }
        });
        
        // Write the results to an attribute 
        String results = value.get();
        if(results != null && !results.isEmpty()){
            flowfile = session.putAttribute(flowfile, "match", results);
        }
        
        // To write the results back out ot flow file

        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        session.transfer(flowfile, SUCCESS);                                                                                                                                                                                                                                                                                                                                                                                                 
    }
    
    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    public static void main(String[] args){

        InputStream content = new ByteArrayInputStream("{\"@timestamp\":\"2017-07-21T02:29:30.000Z\",\"beat\":{\"hostname\":\"BC-VM-1418df5b51e34dfabcc357c96cce26b5\",\"name\":\"BC-VM-1418df5b51e34dfabcc357c96cce26b5\",\"version\":\"3.2.0\"},\"@version\":\"1\",\"host\":\"BC-VM-1418df5b51e34dfabcc357c96cce26b5\",\"type\":\"netstat\",\"exec\":{\"stdout\":\"Active Internet connections (servers and established)\\nProto Recv-Q Send-Q Local Address               Foreign Address             State       PID/Program name    Timer\\ntcp        0      0 0.0.0.0:9999                0.0.0.0:*                   LISTEN      8501/java           off (0.00/0/0)\\ntcp        0      0 0.0.0.0:111                 0.0.0.0:*                   LISTEN      1453/rpcbind        off (0.00/0/0)\\ntcp        0      0 0.0.0.0:22                  0.0.0.0:*                   LISTEN      62825/sshd          off (0.00/0/0)\\ntcp        0      0 127.0.0.1:631               0.0.0.0:*                   LISTEN      9185/cupsd          off (0.00/0/0)\\ntcp        0      0 0.0.0.0:5432                0.0.0.0:*                   LISTEN      6223/postmaster     off (0.00/0/0)\\ntcp        0      0 127.0.0.1:25                0.0.0.0:*                   LISTEN      9082/master         off (0.00/0/0)\\n\",\"exitCode\":0,\"command\":\"netstat\"},\"tags\":[\"beats_input_raw_event\"]}".getBytes());
        //InputStream content = new ByteArrayInputStream("".getBytes());
        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new FlattenMapProcessor());

        // Add properties
        //"proto", "recv", "send", "local", "foreign", "state"
        runner.setProperty(FlattenMapProcessor.TIMESTAMP_FIELD, "@timestamp");
        runner.setProperty(FlattenMapProcessor.DATA_DRIVEN, "false");
        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        //Gson gson = new Gson();
        //String item = "udp4       0      0  *.*                    *.* 1";
        //System.out.println(gson.toJson(item.split("( ){2,}")));
    }
}
