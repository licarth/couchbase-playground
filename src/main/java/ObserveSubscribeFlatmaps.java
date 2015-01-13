import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.loader.StringLoader;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import lombok.extern.log4j.Log4j2;
import net.spy.memcached.internal.BasicThreadFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static com.couchbase.client.java.document.json.JsonArray.from;

@Log4j2
public class ObserveSubscribeFlatmaps {
    public static final String AIRPORT_PAGE = "airport_page";
    public static final String utf8EndToken = "\u02ad";

    private static PebbleEngine engine;
    private Cluster couchbaseCluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.create(), "cb04.goeuro.int:8091");
    private Bucket contentBucket = couchbaseCluster.openBucket("seo-content", "yej_7usPU_eWRup");

    final ExecutorService execA;
    final ExecutorService execB;
    final ExecutorService execC;

    public ObserveSubscribeFlatmaps() {
        this.execA = Executors.newCachedThreadPool(new BasicThreadFactory("execA", false));
        this.execB = Executors.newCachedThreadPool(new BasicThreadFactory("execB", false));
        this.execC = Executors.newCachedThreadPool(new BasicThreadFactory("execC", false));
    }

    public static void main(String[] args) throws IOException {
        new ObserveSubscribeFlatmaps().start();
        System.in.read();
    }

    private void start() {
        contentBucket.async()
                .query(ViewQuery
                                .from(AIRPORT_PAGE, AIRPORT_PAGE)
                                .stale(Stale.TRUE)
                                .startKey(from("fr"))
                                .reduce(false)
                                .limit(10)
                                .endKey(from("fr", utf8EndToken))
                )
                .flatMap(new Func1<AsyncViewResult, Observable<AsyncViewRow>>() {
                    @Override
                    public Observable<AsyncViewRow> call(AsyncViewResult asyncViewResult) {
                        return asyncViewResult.rows();
                    }
                })
                .flatMap(new Func1<AsyncViewRow, Observable<LegacyDocument>>() {
                    @Override
                    public Observable<LegacyDocument> call(AsyncViewRow asyncViewRow) {
                        return asyncViewRow.document(LegacyDocument.class)
//                                .subscribeOn(Schedulers.from(execA))
                                ;
                    }
                })
                .flatMap(new Func1<LegacyDocument, Observable<String>>() {
                    @Override
                    public Observable<String> call(final LegacyDocument legacyDocument) {
                        return getStringObservable(legacyDocument)
                                .subscribeOn(Schedulers.from(execA)) //TO AVOID TIMEOUTS !!!
                                ;
                    }
                })
                .flatMap(new Func1<String, Observable<String>>() {
                    @Override
                    public Observable<String> call(String string) {
                        return slowConsumer(string)
                                .subscribeOn(Schedulers.from(execC))
                                ;
                    }
                }).subscribe(
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
    private Observable<String> slowConsumer(final String string) {
        return Observable.create(new Observable.OnSubscribe<String>() {
            @Override
            public void call(Subscriber<? super String> subscriber) {
                try{
                    Thread.sleep(0);
                    log.info(string);
                    subscriber.onNext(string);
                    subscriber.onCompleted();
                } catch (InterruptedException e) {
                    subscriber.onError(e);
                } finally {
                }
            }
        });
    }


    private String getSync(String key){
        return contentBucket.get(key, LegacyDocument.class).content().toString();
    }

}
