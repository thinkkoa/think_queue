# 介绍
-----

[![npm version](https://badge.fury.io/js/think_queue.svg)](https://badge.fury.io/js/think_queue)
[![Dependency Status](https://david-dm.org/thinkkoa/think_queue.svg)](https://david-dm.org/thinkkoa/think_queue)

A simple queue thinkkoa middleware based on redis.

# Install
-----

```
npm i think_queue --save
```

# API

## queue(name, opts)

Create a queue with `name` and config redis client connection based on `opts`, returns a decent queue instance.

### examples

```
const queue = require('think_queue');

const queue1 = queue('q1', { 
	redis_host: '127.0.0.1',
    redis_port: 6379,
    redis_password: '',
    redis_db: '0',
    redis_timeout: 5000, //try connection timeout

    queue_blocktimeout: 60,
    queue_maxretry: 3 
});
```

### opts

- `blockTimeout`: how long a client should wait for next job (see redis document on blocking command, such as [BLPOP](http://redis.io/commands/BLPOP)), defaults to `60` seconds, `0` to block forever.
- `maxRetry`: how many retries a job can have before being moved to failure queue, defaults to `3`, `0` to disable retry.

## queue.add(data, opts)

Create a job on queue using `data` as payload and allows job specific `opts`, returns a promise that resolve to the created job.

### examples

```
queue.add({ a: 1 }).then(function(job) {
	console.log(job.data); // { a: 1 }
});

queue.add({ a: 1, b: 1 }, { retry: 1, timeout: 120 }).then(function(job) {
	console.log(job.data); // { a: 1, b: 1 }
	console.log(job.retry); // 1
	console.log(job.timeout); // 120
});
```

### opts

- `retry`: set initial retry counter, default to `0`
- `timeout`: set worker timeout in seconds, default to `60`

### job

- `id`: job id
- `data`: payload
- `retry`: current retry count for this job
- `timeout`: how many seconds a worker can run before it's terminated.
- `queue`: which queue this job currently belongs to.


## queue.worker(handler)

Register a handler function that process jobs, and start processing jobs in queue.

### examples

```
queue.worker(function(job, done) {
	setTimeout(function() {
		console.log(job.data);
		done();
	}, 100);
});
```

### done(err);

Must be called to signal the completion of job processing.

If called with an instance of `Error`, then `decent` will assume worker failed to process this job.

Fail jobs are moved back to work queue when they are below retry threshold, otherwise they are moved to failure queue.


## queue.count(name)

Returns a promise that resolve to the queue length of specified queue, default to `work` queue.

### examples

```
queue.count('work').then(function(count) {
	console.log(count); // pending job count
});

queue.count('run').then(function(count) {
	console.log(count); // running job count
});

queue.count('fail').then(function(count) {
	console.log(count); // failed job count
});
```


## queue.get(id)

Returns a promise that resolve to the job itself.

### examples

```
queue.get(1).then(function(job) {
	console.log(job.id); // 1
});
```


## queue.remove(id, name)

Returns a promise that will resolve when job is removed from redis (both job data and job queue). Default queue is `work`.

Note: `remove` does not return the job, use `get` then `remove` instead.

### examples

```
queue.remove(1, 'run').then(function() {
	// job has been removed from redis
});
```


## queue.stop()

Instructs queue worker to terminate gracefully on next loop. See events on how to monitor queue.

### examples

```
// ... setup queue and worker

queue.on('queue stop', function() {
	console.log('queue stopped gracefully');
});
queue.stop();
```


## queue.restart()

Restarts the queue worker loop. See events on how to monitor queue.

### examples

```
// ... setup queue and worker

queue.on('queue start', function() {
	console.log('queue restarted');
});
queue.restart();
```


# Events

`decent` is an instance of `EventEmitter`, so you can use `queue.on('event', func)` as usual.

## queue worker related

- `queue.emit('queue start')`: queue loop has started.
- `queue.emit('queue work', job)`: queue worker begin to process a `job`.
- `queue.emit('queue ok', job)`: queue worker has processed a `job`.
- `queue.emit('queue error', err, job)`: queue worker has failed to processed a `job` and thrown `err`, will retry later.
- `queue.emit('queue failure', err, job)`: queue worker has failed to processed a `job` and thrown `err`, retry limit reached.
- `queue.emit('queue exit', err)`: queue loop has terminated due to unhandled `err`.
- `queue.emit('queue stop')`: queue loop has stopped gracefully.


# License

MIT