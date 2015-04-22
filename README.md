Just messing around with multiprocessing, sending and receiving commands

Example run
```
$ ./executive.py -o reverse:hello ping
INFO:Grunt:Set up Grunt instance
INFO:Executive:Set up Executive instance with pid 12748
INFO:Executive:Sent order reverse with params ['hello']
INFO:Executive:Sent order ping with params None
INFO:main:Sent 2 orders!
INFO:Grunt:Starting with pid 12749
INFO:Executive:Sent order show_results with params None
INFO:Grunt:Results:
[{'order': 'reverse', 'result': 'olleh', 'time_done': 1429694504.793402},
 {'order': 'ping', 'result': True, 'time_done': 1429694504.7937131}]
```
