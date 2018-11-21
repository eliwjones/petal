# petal
Data shuffling

# Quick Start

```
$ python processor.py
$ ls -la aggregated*.csv
-rw-r--r--  1 gigi  staff  1493 Nov 20 23:10 aggregated-transactions1.csv
-rw-r--r--  1 gigi  staff  1146 Nov 20 23:10 aggregated-transactions2.csv
-rw-r--r--  1 gigi  staff  1268 Nov 20 23:10 aggregated-transactions3.csv
```

# Test

```
$ python test_processor.py -v
test_it (__main__.TestProcessor) ... FAIL

======================================================================
FAIL: test_it (__main__.TestProcessor)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "test_processor.py", line 31, in test_it
    self.assertEqual(filtered_results[user_id], expected_result)
AssertionError: {'max': '106063.72', 'sum': '-115763.31', 'min': '-105901.55', 'user_id': '10006 [truncated]... != {'max': '106063.72', 'sum': '-115763.31', 'min': '-115763.31', 'user_id': '10006 [truncated]...
  {'max': '106063.72',
-  'min': '-105901.55',
+  'min': '-115763.31',
   'n': '1155',
   'sum': '-115763.31',
   'user_id': '1000616411022492'}

----------------------------------------------------------------------
Ran 1 test in 2.783s

FAILED (failures=1)

```

NOTE: Presumably, there is another parsing error lurking.
