package ru.store.impl.durability;

import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface ExecutorByTimeout<T extends Runnable> {

    void execute(T task, long timeout, TimeUnit timeUnit);
}
