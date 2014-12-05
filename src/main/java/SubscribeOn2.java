import lombok.extern.log4j.Log4j2;
import net.spy.memcached.internal.BasicThreadFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static rx.schedulers.Schedulers.from;

@Log4j2
public class SubscribeOn2 {

    final ExecutorService execA;
    final ExecutorService execB;
    final ExecutorService execC;

    public SubscribeOn2() {
        this.execA = Executors.newCachedThreadPool(new BasicThreadFactory("execA", false));
        this.execB = Executors.newCachedThreadPool(new BasicThreadFactory("execB", false));
        this.execC = Executors.newCachedThreadPool(new BasicThreadFactory("execC", false));
    }


    public static void main(String[] args) throws IOException {
        new SubscribeOn2().start();
        log.info("Main thread terminated");
//        System.in.read();
    }

    private void start() {

        Observable.just(1,2)
                .flatMap(new Func1<Integer, Observable<String>>() {
                    @Override
                    public Observable<String> call(Integer integer) {
                        log.info("this is on execA.");
                        return simpleObservable()
                                .observeOn(from(execB))
                                .doOnNext(new Action1<String>() {
                                    @Override
                                    public void call(String s) {
                                        log.info("This is a notification --> go to execB");
                                    }
                                })
//                                .subscribeOn(from(execB))
                                ;
                    }
                })
                .observeOn(from(execC))
                .subscribeOn(from(execA))
                .subscribe(new Action1<String>() {
            @Override
            public void call(String s) {
                log.info("This is processing --> execC");
            }
        });

    }

    private Observable<String> simpleObservable() {
        return Observable.create(
                new Observable.OnSubscribe<String>() {
                    @Override
                    public void call(Subscriber<? super String> subscriber) {
                        log.info("This is executed on the subscribeOn() scheduler.");
                        subscriber.onNext("string");
                        subscriber.onCompleted();
                    }
                });
    }

}
