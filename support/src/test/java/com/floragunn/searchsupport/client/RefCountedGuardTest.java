package com.floragunn.searchsupport.client;

import org.elasticsearch.core.RefCounted;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class RefCountedGuardTest {

    @Mock
    private RefCounted refCountedOne;
    @Mock
    private RefCounted refCountedTwo;
    @Mock
    private RefCounted refCountedThree;
    @Mock
    private RefCounted refCountedFour;
    @Mock
    private RefCounted refCountedFive;

    private RefCountedGuard<RefCounted> guard;

    @Before
    public void setUp() {
        this.guard = new RefCountedGuard<>();
    }

    @Test
    public void shouldNotInvokeAnyMethodOnTrackedObject() {
        guard.add(refCountedOne);
        guard.add(refCountedTwo);
        guard.add(refCountedThree);

        verifyNoInteractions(refCountedOne, refCountedTwo, refCountedThree);
    }

    @Test
    public void shouldReleaseResources() {
        guard.add(refCountedOne);

        guard.release();

        verify(refCountedOne).decRef();
        verifyNoMoreInteractions(refCountedOne);
    }

    @Test
    public void shouldReleaseResourcesOnlyOnce() {
        guard.add(refCountedOne);
        guard.release();

        guard.release();

        verify(refCountedOne, times(1)).decRef();
        verifyNoMoreInteractions(refCountedOne);
    }

    @Test
    public void shouldReleaseMultipleResources() {
        guard.add(refCountedOne);
        guard.add(refCountedTwo);
        guard.add(refCountedThree);

        guard.release();

        verify(refCountedOne).decRef();
        verify(refCountedTwo).decRef();
        verify(refCountedThree).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree);
    }

    @Test
    public void shouldReleaseResourceOnClose() {
        guard.add(refCountedOne);
        guard.add(refCountedTwo);
        guard.add(refCountedThree);

        guard.close();

        verify(refCountedOne).decRef();
        verify(refCountedTwo).decRef();
        verify(refCountedThree).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree);
    }

    @Test
    public void shouldNotReleaseResourcesOnCloseWhenResourcesWasPreviouslyReleased() {
        guard.add(refCountedOne);
        guard.add(refCountedTwo);
        guard.add(refCountedThree);
        guard.release();

        guard.close();

        verify(refCountedOne, times(1)).decRef();
        verify(refCountedTwo, times(1)).decRef();
        verify(refCountedThree, times(1)).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree);
    }

    @Test
    public void shouldReleaseResourceMultipleTimes() {
        guard.add(refCountedOne);
        guard.add(refCountedTwo);
        guard.add(refCountedThree);
        guard.release(); // first release
        guard.add(refCountedFour);
        guard.add(refCountedFive);

        guard.release(); // second release

        verify(refCountedOne, times(1)).decRef();
        verify(refCountedTwo, times(1)).decRef();
        verify(refCountedThree, times(1)).decRef();
        verify(refCountedFour, times(1)).decRef();
        verify(refCountedFive, times(1)).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree, refCountedFour, refCountedFive);

    }

    @Test
    public void shouldReleaseResourceSecondTimeOnClode() {
        guard.add(refCountedOne);
        guard.add(refCountedTwo);
        guard.add(refCountedThree);
        guard.release(); // first release
        guard.add(refCountedFour);
        guard.add(refCountedFive);

        guard.close(); // second release

        verify(refCountedOne, times(1)).decRef();
        verify(refCountedTwo, times(1)).decRef();
        verify(refCountedThree, times(1)).decRef();
        verify(refCountedFour, times(1)).decRef();
        verify(refCountedFive, times(1)).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree, refCountedFour, refCountedFive);

    }

    @Test
    public void shouldReleaseResourceFiveTimes() {
        guard.add(refCountedOne);
        guard.release(); // 1
        guard.add(refCountedTwo);
        guard.release(); // 2
        guard.add(refCountedThree);
        guard.release(); // 3
        guard.add(refCountedFour);
        guard.release(); // 4
        guard.add(refCountedFive);

        guard.close(); // 5

        verify(refCountedOne, times(1)).decRef();
        verify(refCountedTwo, times(1)).decRef();
        verify(refCountedThree, times(1)).decRef();
        verify(refCountedFour, times(1)).decRef();
        verify(refCountedFive, times(1)).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree, refCountedFour, refCountedFive);
    }

    @Test
    public void shouldIgnoreExceptionDuringReleasingResources() {
        doThrow(new IllegalStateException("For test purposes")).when(refCountedThree).decRef();
        guard.add(refCountedOne);
        guard.release(); // 1
        guard.add(refCountedTwo);
        guard.release(); // 2
        guard.add(refCountedThree);
        guard.release(); // 3
        guard.add(refCountedFour);
        guard.release(); // 4
        guard.add(refCountedFive);

        guard.close(); // 5

        verify(refCountedOne, times(1)).decRef();
        verify(refCountedTwo, times(1)).decRef();
        verify(refCountedThree, times(1)).decRef();
        verify(refCountedFour, times(1)).decRef();
        verify(refCountedFive, times(1)).decRef();
        verifyNoMoreInteractions(refCountedOne, refCountedTwo, refCountedThree, refCountedFour, refCountedFive);
    }
}