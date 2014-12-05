import lombok.extern.log4j.Log4j2;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;

@Log4j2
public class SubscribeOn1 {

    public static void main(String[] args) throws IOException {
        new SubscribeOn1().simple();
        System.in.read();
    }

    private void simple() {

        Observable.create(
                new Observable.OnSubscribe<Object>() {
                    @Override
                    public void call(Subscriber<? super Object> subscriber) {
                        log.info("This is executed on the subscribeOn() scheduler.");
                    }
                })
                .subscribeOn(Schedulers.io())
                .subscribe();


    }

}
