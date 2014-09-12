package org.apache.uima.rabbit;

import static com.rabbitmq.client.MessageProperties.PERSISTENT_BASIC;
import static org.apache.uima.rabbit.RabbitReader.snippetize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitWriter extends JCasAnnotator_ImplBase {

    public final static boolean DURABLE = true;

    private Channel sendChannel;

    @ConfigurationParameter(name = RabbitReader.PARAM_AMQP_URI, //
    description = "'localhost', or amqpUri, amqp://userName:password@hostName:portNumber/")
    private final String amqpUri = null;

    @ConfigurationParameter(name = RabbitReader.PARAM_QUEUE, description = "Name of the Rabbit queue")
    private final String queue = null;

    @ConfigurationParameter(name = RabbitReader.PARAM_MAX_QUEUE_SIZE, mandatory = false, defaultValue = "1000",//
    description = "The maximum size of the queue. Values for RabbitWriter and Reader should match")
    private int maxQueueSize;

    @Override
    public void initialize(UimaContext context)
            throws ResourceInitializationException {
        super.initialize(context);

        try {
            // setup connection
            ConnectionFactory factory = new ConnectionFactory();
            if (amqpUri.equals("localhost")) {
                factory.setHost("localhost");
            } else {
                factory.setUri(amqpUri);
            }
            Connection connection = factory.newConnection();
            getLogger().info(" [RabbitWriter] Connected to Rabbit");

            // setup channels
            sendChannel = connection.createChannel();
            Map<String, Object> args = new HashMap<String, Object>();
            args.put("x-max-length", maxQueueSize);
            sendChannel.queueDeclare(queue, DURABLE, false, false, args);

        } catch (Exception e) {
            throw new ResourceInitializationException(e);
        }
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        try {
            sendChannel.basicPublish("", queue, PERSISTENT_BASIC,
                    serialize(jCas.getCas()));
            getLogger().debug(
                    " [RabbitWriter] '"
                            + snippetize(jCas.getDocumentText(), 20) + "'");
        } catch (IOException e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

    @Override
    public void collectionProcessComplete()
            throws AnalysisEngineProcessException {
        try {
            sendChannel.close();
        } catch (IOException e) {// nope
        }
    }

    public static void deserialize(CAS cas, byte[] byteArray)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        ObjectInputStream in = new ObjectInputStream(bis);

        Serialization.deserializeCAS(cas, in);
        in.close();
    }

    public static byte[] serialize(CAS cas) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        Serialization.serializeWithCompression(cas, out);
        out.close();
        return bos.toByteArray();
    }
}
