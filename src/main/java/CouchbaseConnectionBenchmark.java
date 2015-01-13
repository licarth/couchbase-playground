import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.ViewQuery;
import com.mitchellbosecke.pebble.PebbleEngine;
import lombok.extern.log4j.Log4j2;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

@Log4j2
public class CouchbaseConnectionBenchmark {
    public static final String KEY = "key";
    public static final String utf8EndToken = "\u02ad";
    public static final DefaultCouchbaseEnvironment ENVIRONMENT = DefaultCouchbaseEnvironment
            .builder()
            .connectTimeout(25000)
            .viewEndpoints(50)
            .queryEndpoints(5)
            .kvEndpoints(50)
            .build();

    public static final String REMOTE_URL = "cb04.goeuro.int:8091";
    public static final String LOCAL_URL = "localhost:8091";

    private static PebbleEngine engine;
    private Cluster couchbaseCluster = CouchbaseCluster.create(ENVIRONMENT, REMOTE_URL);
    private Bucket contentBucket = couchbaseCluster.openBucket("seo-content", "yej_7usPU_eWRup");
    private MetricRegistry metricRegistry = new MetricRegistry();
    final Timer viewResponseTimer = metricRegistry.timer(name(CouchbaseConnectionBenchmark.class, "couchbase-view-response"));
    final Timer documentFetchTimer = metricRegistry.timer(name(CouchbaseConnectionBenchmark.class, "couchbase-document-fetch"));

    public CouchbaseConnectionBenchmark() {
        ConsoleReporter.forRegistry(metricRegistry).build().start(2, TimeUnit.SECONDS);

    }

    public static void main(String[] args) throws IOException {
        new CouchbaseConnectionBenchmark().start();
        System.in.read();
    }

    private void start() {
        contentBucket.async()
                .query(ViewQuery
                                .from(KEY, KEY)
                                .startKey("")
                                .reduce(false)
                                .limit(1)
                                .endKey(utf8EndToken)
                )
                .repeat(100)
                .flatMap(new Func1<AsyncViewResult, Observable<AsyncViewRow>>() {
                    @Override
                    public Observable<AsyncViewRow> call(AsyncViewResult asyncViewResult) {
                        final Timer.Context time = viewResponseTimer.time();
                        return asyncViewResult.rows().doOnTerminate(new Action0() {
                            @Override
                            public void call() {
                                time.stop();
                            }
                        });
                    }
                })
                .flatMap(new Func1<AsyncViewRow, Observable<LegacyDocument>>() {
                    @Override
                    public Observable<LegacyDocument> call(AsyncViewRow asyncViewRow) {
                        final Timer.Context time = documentFetchTimer.time();
                        return asyncViewRow
                                .document(LegacyDocument.class)
                                .doOnTerminate(new Action0() {
                                    @Override
                                    public void call() {
                                        time.stop();
                                    }
                                })
//                                .subscribeOn(Schedulers.from(execA))
                                ;
                    }
                })
                .flatMap(new Func1<LegacyDocument, Observable<String>>() {
                    @Override
                    public Observable<String> call(LegacyDocument doc) {
                        return printer(doc.content().toString())
                                .subscribeOn(Schedulers.io())
                                ;
                    }
                })
//                .observeOn(Schedulers.from(execC))
                .subscribe(
                        new Subscriber<String>() {
                            @Override
                            public void onCompleted() {
                                log.info("COMPLETE");
                            }

                            @Override
                            public void onError(Throwable e) {
                                log.info("ERROR", e);
                            }

                            @Override
                            public void onNext(String s) {
                            }
                        });
    }

    private Observable<String> getStringObservable(final LegacyDocument legacyDocument) {
        return Observable.create(new Observable.OnSubscribe<String>() {
            @Override
            public void call(Subscriber<? super String> subscriber) {
                subscriber.onNext(getSync(legacyDocument.id()));
                subscriber.onCompleted();
            }
        });
    }
    private Observable<String> printer(final String string) {
        return Observable.create(new Observable.OnSubscribe<String>() {
            @Override
            public void call(Subscriber<? super String> subscriber) {
                try {
                    log.info(string);
                    subscriber.onNext(string);
                    subscriber.onCompleted();
                } finally {
                }
            }
        });
    }


    private String getSync(String key){
        return contentBucket.get(key, LegacyDocument.class).content().toString();
    }

}
