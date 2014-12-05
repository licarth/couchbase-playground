import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class OldClientThreadsExperiment {

    public static void main(final String[] args) throws IOException, URISyntaxException {
        ArrayList<URI> hostsList = new ArrayList<URI>() {{
            add(new URI("http://cb04.goeuro.int:8091/pools"));
            add(new URI("http://cb05.goeuro.int:8091/pools"));
        }};

        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        cfb.setOpQueueMaxBlockTime(50000); // wait up to 5 seconds when trying to enqueue an operation
//        cfb.setViewTimeout(30); // set the timeout to 30 seconds
        cfb.setViewWorkerSize(15); // use 15 worker threads instead of one
        cfb.setViewConnsPerNode(40); // allow 20 parallel http connections per node in the cluster
        CouchbaseConnectionFactory connectionFactory = cfb.buildCouchbaseConnection(hostsList, "seo-content", "yej_7usPU_eWRup");
        CouchbaseClient couchbaseClient = new CouchbaseClient(connectionFactory);

        while (true){
            couchbaseClient.asyncGet("test");
        }


//        System.in.read();

    }

}
