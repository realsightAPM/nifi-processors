/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.realsight.nifi.processors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.DoubleAdder;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author phillip
 */
@SideEffectFree
@Tags({"JSON", "NIFI ROCKS"})
@CapabilityDescription("Fetch value from json path.")
public class PrometheusProcessor extends AbstractProcessor {
   
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final Logger logger = LoggerFactory.getLogger(PrometheusProcessor.class);

    public static final String MATCH_ATTR = "match";
    
    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("Json Path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(JSON_PATH);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        
        final AtomicReference<String> value = new AtomicReference<>();
        final CollectorRegistry registry = new CollectorRegistry();
        final Gson gson = new Gson();
        FlowFile flowfile = session.get();
        
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    String json = IOUtils.toString(in);
                    System.err.println(json);
                    Map<String, Object> datas = gson.fromJson(json, new TypeToken<Map<String,Object>>(){}.getType());
                    HashMap<String, Double> metrics = new HashMap<>();
                    ArrayList<String> labelnames = new ArrayList<>();
                    ArrayList<String> labelvalues = new ArrayList<>();

                    for(Map.Entry<String, Object> entry: datas.entrySet()) {
                        if( entry.getValue() instanceof String ) {
                            String key = entry.getKey();
                            String value = entry.getValue().toString();
                            if( StringUtils.isNumeric(value) ) {
                                //System.err.println(entry.getValue());
                                metrics.put(key, Double.parseDouble(value));
                            }
                            else {
                                if(key.startsWith("@")){
                                    continue;
                                }
                                labelnames.add(key);
                                labelvalues.add(value);
                            }
                        }
                    }
                    for(String key: metrics.keySet()){
                        Double value = metrics.get(key);
                        if(!value.isNaN()){
                            continue;
                        }
                        System.err.println(value);
                        Gauge item = Gauge.build().name(key).labelNames(labelnames.toArray(new String[0])).help(key).register(registry);
                        item.set(value);
                        item.labels(labelvalues.toArray(new String[0]));
                    }
                    PushGateway pg = new PushGateway("127.0.0.1:9091");
                    pg.pushAdd(registry, "my_batch_job");
                    //value.set(json);
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                }
            }
        });


        /*
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
        */
        
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

    public void executeBatchJob() throws Exception {
        CollectorRegistry registry = new CollectorRegistry();
        Gauge duration = Gauge.build()
                .name("my_batch_job_duration_seconds").help("Duration of my batch job in seconds.").register(registry);
        Gauge.Timer durationTimer = duration.startTimer();
        try {
            // Your code here.

            // This is only added to the registry after success,
            // so that a previous success in the Pushgateway isn't overwritten on failure.
            Gauge lastSuccess = Gauge.build()
                    .name("my_batch_job_last_success").help("Last time my batch job succeeded, in unixtime.").register(registry);
            lastSuccess.setToCurrentTime();
        } finally {
            durationTimer.setDuration();
            PushGateway pg = new PushGateway("127.0.0.1:9091");
            pg.pushAdd(registry, "my_batch_job");
        }
    }
}
