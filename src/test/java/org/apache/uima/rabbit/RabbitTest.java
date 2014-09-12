package org.apache.uima.rabbit;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;
import static org.apache.uima.rabbit.RabbitReader.PARAM_AMQP_URI;
import static org.apache.uima.rabbit.RabbitReader.PARAM_QUEUE;
import static org.apache.uima.rabbit.RabbitReader.PARAM_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitTest {

    public static final String AMQP_URI = null; // localhost
    public static final String TEST_CHANNEL = "test";

    @Test
    public void test() throws Exception {

        // delete test channel
        ConnectionFactory factory = new ConnectionFactory();
        if (AMQP_URI == null) {
            factory.setHost("localhost");
        } else {
            factory.setUri(AMQP_URI);
        }
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.queueDelete(TEST_CHANNEL);
        } catch (Exception e) {
            System.out.println(e.getCause().getMessage());
            assertTrue(e.getCause().getMessage()
                    .indexOf("no queue '" + TEST_CHANNEL) > -1);
        }

        // queue some documents
        runPipeline(
                createReader(TestReader.class),
                createEngine(RabbitWriter.class, PARAM_AMQP_URI, AMQP_URI,
                        PARAM_QUEUE, TEST_CHANNEL));

        // retrieve them
        runPipeline(
                createReader(RabbitReader.class, PARAM_AMQP_URI, AMQP_URI,
                        PARAM_QUEUE, TEST_CHANNEL, PARAM_TIMEOUT, 1),
                createEngine(TestEngine.class));
    }

    /** Creates 10 test documents */
    public static class TestReader extends JCasCollectionReader_ImplBase {

        Queue<String> queue = new LinkedList<String>();

        @Override
        public void initialize(UimaContext context)
                throws ResourceInitializationException {
            super.initialize(context);
            for (int i = 0; i < 10; i++) {
                queue.add("document " + i);
            }
        }

        public boolean hasNext() throws IOException, CollectionException {
            return !queue.isEmpty();
        }

        @Override
        public void getNext(JCas jCas) throws IOException, CollectionException {
            String txt = queue.poll();
            jCas.setDocumentText(txt);
            getLogger().debug("TestReader reading:: " + txt);
        }

        public Progress[] getProgress() {
            return null;
        }
    }

    /** Checks the 10 test documents created by {@link TestReader} */
    public static class TestEngine extends JCasAnnotator_ImplBase {

        private int cnt;

        @Override
        public void initialize(UimaContext context)
                throws ResourceInitializationException {
            super.initialize(context);
            cnt = 0;
        }

        @Override
        public void process(JCas jCas) throws AnalysisEngineProcessException {
            assertEquals("document " + cnt++, jCas.getDocumentText());
            getLogger()
                    .debug("TestEngine checking:: " + jCas.getDocumentText());
        }

        @Override
        public void collectionProcessComplete()
                throws AnalysisEngineProcessException {
            assertEquals(10, cnt);
        }
    }
}
