package com.realsight.nifi.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import rocks.nifi.examples.processors.JsonProcessor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jiajia on 2017/7/7.
 */
public class PrometheusProcessorTest {
    @Test
    public void onTrigger() throws Exception {
        InputStream content = new ByteArrayInputStream(
                ("{\"@version_f\":\"1\",\"beat_name_s\":\"jiajialaogongdeMacBook-Pro.local\",\"system_process_memory_share_f\":\"0\",\"@timestamp_s\":\"2017-07-14T01:52:32.903Z\",\"system_process_cmdline_s\":\"/System/Library/CoreServices/Finder.app/Contents/MacOS/Finder\",\"system_process_memory_rss_pct_f\":\"0.0057\",\"system_process_cpu_start_time_s\":\"2017-07-04T01:28:39.364Z\",\"type\":\"metricsets\",\"system_process_memory_rss_bytes_f\":\"48644096\",\"metricset_module_s\":\"system\",\"system_process_cpu_total_pct_f\":\"0.0\",\"metricset_rtt_f\":\"49379\",\"system_process_pgid_f\":\"894\",\"system_process_name_s\":\"Finder\",\"host_s\":\"jiajialaogongdeMacBook-Pro.local\",\"@version\":\"1\",\"system_process_pid_f\":\"894\",\"type_s\":\"metricsets\",\"system_process_username_s\":\"jiajia\",\"system_process_state_s\":\"running\",\"system_process_ppid_f\":\"1\",\"beat_version_s\":\"5.4.3\",\"metricset_name_s\":\"process\",\"beat_hostname_s\":\"jiajialaogongdeMacBook-Pro.local\",\"tags\":[\"beats_input_raw_event\"],\"@timestamp\":\"2017-07-14T01:52:32.903Z\",\"system_process_memory_size_f\":\"4764930048\"}"
                ).getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new PrometheusProcessor());

        // Add properties
        runner.setProperty(JsonProcessor.JSON_PATH, "$.hello");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(JsonProcessor.SUCCESS);


    }

}