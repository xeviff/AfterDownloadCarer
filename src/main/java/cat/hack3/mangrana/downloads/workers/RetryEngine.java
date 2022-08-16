package cat.hack3.mangrana.downloads.workers;

import org.apache.commons.lang.StringUtils;

import java.rmi.UnexpectedException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static cat.hack3.mangrana.utils.Output.log;
import static cat.hack3.mangrana.utils.Output.logDate;

public class RetryEngine<D> {

    private final int minutesToWait;
    private final int childrenMustHave;
    private final Function<D, List<D>> childrenRetriever;

    public RetryEngine(int minutesToWait) {
        this(minutesToWait, 0, null);
    }
    public RetryEngine(int minutesToWait, int childrenMustHave, Function<D, List<D>> childrenRetriever) {
        this.minutesToWait = minutesToWait;
        this.childrenMustHave = childrenMustHave;
        this.childrenRetriever = childrenRetriever;
    }

    public D tryUntilGotDesired(Supplier<D> tryToGet) throws UnexpectedException {
        D desired = null;
        boolean waitForChildren = childrenMustHave > 0;
        while (Objects.isNull(desired)) {
            desired = tryToGet.get();
            if (Objects.isNull(desired)) {
                log("couldn't find it");
                waitBeforeNextRetry(minutesToWait, null);
            } else if (waitForChildren) {
                while (waitForChildren) {
                    List<D> children = childrenRetriever.apply(desired);
                    if (children.size() < childrenMustHave) {
                        log("there is no enough child elements yet");
                        waitBeforeNextRetry(minutesToWait, null);
                    } else {
                        int shorterTime = minutesToWait / 3;
                        waitBeforeNextRetry(shorterTime, "waiting a bit more for courtesy: " + shorterTime + "min");
                        waitForChildren = false;
                    }
                }
            }
        }
        log("found desired element and returning it");
        return desired;
    }


    public void waitBeforeNextRetry(int currentMinutesToWait, String forcedMessage) throws UnexpectedException {
        String msg = StringUtils.isNotEmpty(forcedMessage)
                ? forcedMessage
                : "waiting "+currentMinutesToWait+" minutes before the next try";
        log(msg);
        logDate();
        try {
            TimeUnit.MINUTES.sleep(currentMinutesToWait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnexpectedException("failed TimeUnit.MINUTES.sleep");
        }
    }

}
