package ru.store.impl.durability;

/**
 * Implementa retrier pattern
 *
 * @author Dmitrii Shakshin d.shakshin@gmail.com
 */
public class Retrier {

    private final Task task;
    private int countRetry;

    public Retrier(int countRetry, Task task) {
        this.countRetry = countRetry;
        this.task = task;
    }

    public void run() throws Throwable {
        countRetry--;
        Throwable lastException = null;
        for (int i = 0; i < countRetry; i++) {
            try {
                task.run();
            } catch (Throwable e) {
                lastException = e;
                continue;
            }
            lastException = null;
            break;
        }

        if (lastException != null) {
            throw lastException;
        }
    }
}
