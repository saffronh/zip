Files attached:

zipline.py - Implements Zip and basic ZipScheduler
schedulers.py - Implements 3 modified/enhanced schedulers
tester.py - Tests different schedulers for performance
unittests.py - Unit tests


Results
-------

Tests show that schedulers have similar performance - and that which to choose probably depends on what you're optimizing for. E.g. if the priority is for emergency filghts to be answered ASAP, ZipScheduler_NextOrd (just takes the next order in range of the zip) is probably best. On average, regular scheduler probably performs best across all metrics.
