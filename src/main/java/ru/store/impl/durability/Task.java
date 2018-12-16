package ru.store.impl.durability;

@FunctionalInterface
public interface Task {
    void run() throws Exception;
}
