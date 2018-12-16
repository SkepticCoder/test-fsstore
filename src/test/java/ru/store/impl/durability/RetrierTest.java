package ru.store.impl.durability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RetrierTest {

    @Test
    void testSuccessExecuteAfterSomeFails() throws Throwable {
        Task task = mock(Task.class);
        doThrow(new RuntimeException())
                .doThrow(new RuntimeException())
                .doNothing()
                .when(task).run();

        new Retrier(4, task).run();

        verify(task, times(3)).run();
    }

    @Test
    void testStopRetryAfterFail()  {
        assertThrows(RuntimeException.class, () -> {        Task task = mock(Task.class);
            for(int i = 0; i < 4; i++) {
                doThrow(new RuntimeException())
                        .when(task).run();
            }

            new Retrier(3, task).run();

            verify(task, times(3)).run();
        });

    }
}