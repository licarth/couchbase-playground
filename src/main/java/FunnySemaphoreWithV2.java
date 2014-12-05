import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.loader.StringLoader;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import lombok.extern.log4j.Log4j2;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;

@Log4j2
public class FunnySemaphoreWithV2 {
    private static PebbleEngine engine;

    public static void main(String[] args) throws IOException {
//        Cluster couchbaseCluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.create(), "cb04.goeuro.int:8091");
        Cluster couchbaseCluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.create(), "localhost:8091");
//        Bucket bucket = couchbaseCluster.openBucket("seo-content", "yej_7usPU_eWRup");
        final Bucket bucket = couchbaseCluster.openBucket("seo-sample-routes", "JzpNIVK_4ro");

        final PebbleEngine pebbleEngine = getEngine();

        final Semaphore semaphore = new Semaphore(1, true);

        Observable.just("0000000000000001-directtrain-0001", "0000000000000001-directtrain-0001")
                .flatMap(new Func1<String, Observable<?>>() {
                    @Override
                    public Observable<LegacyDocument> call(final String docId) {
                        return Observable.create(new Observable.OnSubscribe<LegacyDocument>() {
                            @Override
                            public void call(Subscriber<? super LegacyDocument> subscriber) {
                                Observable.just(1,2)
                                        .flatMap(new Func1<Integer, Observable<LegacyDocument>>() {
                                            @Override
                                            public Observable<LegacyDocument> call(Integer integer) {
                                                return bucket.async().get(docId, LegacyDocument.class)
                                                        .observeOn(Schedulers.computation());
                                            }
                                        })

                                        .flatMap(new Func1<LegacyDocument, Observable<HashMap<String, LegacyDocument>>>() {
                                            @Override
                                            public Observable<HashMap<String, LegacyDocument>> call(LegacyDocument LegacyDocument) {
                                                return Observable.just(new HashMap<String, com.couchbase.client.java.document.LegacyDocument>());
                                            }
                                        })
//                                        .toMap(
//                                                new Func1<Map<String, LegacyDocument>, String>() {
//                                                    @Override
//                                                    public String call(Map<String, LegacyDocument> LegacyDocument) {
//                                                        return "";
//                                                    }
//                                                }, new Func1<Map<String, LegacyDocument>, Map<String, LegacyDocument>>() {
//                                                    @Override
//                                                    public Map<String, LegacyDocument> call(Map<String, LegacyDocument> doc) {
//                                                        try {
//                                                            return doc;
//                                                        } catch (Exception e) {
//                                                            e.printStackTrace();
//                                                        }
//                                                        return null;
//                                                    }
//                                                }
//                                        )
                                .subscribe(new Action1<Map<String, LegacyDocument>> () {

                                    public void call(Map<String, LegacyDocument> doc) {
                                        try {
                                            new Semaphore(1, true).acquire();
                                            log.info(doc);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        } finally {
                                        }

                                    }
                                });
                            }
                        });
                    }
                })
                .toBlocking().first();

//        System.in.read();
    }


    public static PebbleEngine getEngine() {
        StringLoader loader = new StringLoader();
        PebbleEngine pebbleEngine = new PebbleEngine(loader);
//        pebbleEngine.setLoader(templateLoader);
        pebbleEngine.setDefaultLocale(new Locale("en"));

        // No template cache,
//        // TODO reconsider having one.
        Cache<String, PebbleTemplate> templateCache = CacheBuilder.newBuilder().maximumSize(0).build();
        pebbleEngine.setTemplateCache(null);

        return pebbleEngine;
    }
}
