/**
 *
 * @author     richen
 * @copyright  Copyright (c) 2017 - <richenlin(at)gmail.com>
 * @license    MIT
 * @version    17/10/10
 */
const redis = require('redis');
const lib = require('think_lib');
var inherits = require('util').inherits;
const EventEmitter = require('events').EventEmitter;

/**
 * Create an instance of Queue
 * 
 * @param {any} name 
 * @param {any} [options={}] 
 */
/*eslint-disable func-style */
function Queue(name, options = {}) {
    // allow call as function
    if (!(this instanceof Queue)) {
        return new Queue(name, options);
    }
    if (!name) {
        throw new Error('queue name is required.');
    }

    this.config = {
        blockTimeout: options.queue_blocktimeout || 60,
        maxRetry: options.queue_maxretry || 3
    };

    //redis client
    this.opts = {
        host: options.redis_host || '127.0.0.1',
        port: options.redis_port || 6379,
        auth_pass: options.redis_password || '',
        db: options.redis_db || '0',
        connect_timeout: options.redis_timeout || '0',
    };
    this.client = redis.createClient(this.opts);
    //blocking client
    this.bclient = redis.createClient(this.opts);

    //queue configure
    this.name = name;
    this.prefix = `THINKQUEUE:${this.name}`;
    this.workQueue = `THINKQUEUE:work`;
    this.runQueue = `THINKQUEUE:run`;
    this.failQueue = `THINKQUEUE:fail`;

    //status code
    this.status_timeout = 1;
    //queue status
    this.shutdown = false;
    this.running = false;
}

// EventEmitter
inherits(Queue, EventEmitter);

/**
 * Add a job onto the work queue, overwrite duplicate job
 * 
 * @param {any} data  data for worker to process
 * @param {any} [opts={}] job options
 * @returns 
 */
Queue.prototype.add = function (data, opts = {}) {
    //note that queue id always increment, even if we don't use it
    return this.nextId().then(id => {
        if (!data || !lib.isObject(data)) {
            throw new Error('job data payload must be an JSON object');
        }

        // job structure
        let job = {
            id: opts.id || id,
            data: data,
            retry: opts.retry || 0,
            timeout: opts.timeout || 60,
            queue: this.workQueue
        };

        //format job for redis, invalid data will reject promise
        let rjob = this.toClient(job);

        //add job as hash, push its id onto queue
        //note that overwrite existing job will requeue job id
        let self = this;
        return new Promise(function (resolve, reject) {
            self.client.multi().hmset(self.prefix + ':' + rjob.id, rjob).lrem(self.workQueue, 1, rjob.id).lpush(self.workQueue, rjob.id).exec(function (err, res) {
                // client error
                if (err) {
                    reject(err[0]);
                    // command failure
                    // we need to check commands are returning expected result
                    // as err is null in this case, ref: http://git.io/bT5C4Q
                    // duplicate job id should be purged
                } else if (isNaN(parseInt(res[1], '10')) || res[1] > 1) {
                    err = new Error('fail to remove duplicate job id from queue');
                    reject(err);
                } else {
                    resolve(job);
                }
            });
        });
    });
};

/**
 *  Remove a job from queue given the job id
 * 
 * @param {any} id 
 * @param {any} name 
 * @returns 
 */
Queue.prototype.remove = function (id, name) {
    name = name || 'work';
    let self = this;

    //job done, remove it
    return new Promise(function (resolve, reject) {
        if (!self[`${name}Queue`]) {
            reject(new Error('invalid queue name'));
            return;
        }

        self.client.multi().lrem(self[name + 'Queue'], 1, id).del(self.prefix + ':' + id).exec(function (err, res) {
            // see prototype.add on why verbose checks are needed
            // client error
            if (err) {
                // only return the first error
                reject(err[0]);

                // command error
            } else if (res[0] != 1) {
                err = new Error('job id missing from queue');
                reject(err);
            } else if (res[1] != 1) {
                err = new Error('job data missing from redis');
                reject(err);
            } else {
                resolve();
            }
        });
    });
};

/**
 * Report the current number of jobs in work queue
 * 
 * @param {any} name 
 * @returns 
 */
Queue.prototype.count = function (name) {
    name = name || 'work';
    let self = this;

    return new Promise(function (resolve, reject) {
        if (!self[`${name}Queue`]) {
            reject(new Error('invalid queue name'));
            return;
        }

        self.client.llen(self[`${name}Queue`], function (err, res) {
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
};

/**
 * Return job data without removing it from redis
 * 
 * @param {any} id 
 * @returns 
 */
Queue.prototype.get = function (id) {
    let self = this;

    return new Promise(function (resolve, reject) {
        if (!id) {
            reject(new Error('job id required'));
            return;
        }

        //get job
        self.client.hgetall(`${self.prefix}:${id}`, function (err, job) {
            //client error
            if (err) {
                reject(err);
            } else if (job === null) {
                reject(new Error('job data missing from redis'));
            } else {
                //format job for client, handle invalid job
                try {
                    job = self.fromClient(job);
                } catch (e) {
                    reject(e);
                }
                resolve(job);
            }
        });
    });
};


/**
 *  Start listening for jobs on queue, handle worker function error
 * 
 */
Queue.prototype.start = function () {
    this.running = true;
    this.emit('queue start');
    this.run();
};

/**
 * Stop queue processing gracefully
 * 
 */
Queue.prototype.stop = function () {
    this.shutdown = true;
};

/**
 * Restart queue processing
 * 
 */
Queue.prototype.restart = function () {
    //prevent duplicate worker
    if (this.running) {
        throw new Error('worker is already running');
    }
    this.start();
};

/**
 * Register a handler function that process each job
 * 
 * @param {any} handler 
 */
Queue.prototype.worker = function (handler) {
    if (!lib.isFunction(handler)) {
        throw new Error('job handler must be a function');
    }

    if (this.handler) {
        throw new Error('job handler can only registered once');
    }

    this.handler = handler;
    //once the handler is registered, we can start processing jobs
    this.start();
};

/**
 * Repeatedly retrieve new job from work queue then process it
 * 
 */
Queue.prototype.run = function () {
    let self = this;
    //loop
    this.recoverJob().then(res => {
        return this.readJob(res);
    }).then(job => {
        return this.handleJob(job);
    }).then(function () {
        //handle graceful shutdown
        if (self.shutdown) {
            self.shutdown = false;
            self.running = false;
            self.emit('queue stop');
            return;
        }
        //tail recursion
        self.run();
    }, function (err) {
        //exit queue
        self.running = false;
        self.emit('queue exit', err);
    });
};

/**
 * Wait for job on work queue, loop until found
 * 
 * @param {any} res 
 * @returns 
 */
Queue.prototype.readJob = function (res) {
    //first run, or blocking timeout
    if (!res || res === this.status_timeout) {
        return this.nextJob().then(r => {
            return this.readJob(r);
        });
    } else { //pass on job object
        return Promise.resolve(res);
    }
};

/**
 * Process job with handler
 * 
 * @param {any} job 
 * @returns 
 */
Queue.prototype.handleJob = function (job) {
    let self = this, timeOut;

    //this promise always resolve, errors are handled
    return new Promise(function (resolve, reject) {
        //start working on job
        self.emit('queue work', job);

        if (!self.handler) {
            reject(new Error('job handler must be registered first'));
            return;
        }
        //support job timeout
        if (job.timeout > 0) {
            timeOut = setTimeout(function () {
                reject(new Error('job timeout threshold reached'));
            }, job.timeout * 1000);
        }

        //callback function for async worker
        let done = function (input) {
            if (input instanceof Error) {
                reject(input);
            } else {
                resolve(input);
            }
        };

        //catch worker error
        try {
            self.handler(job, done);
        } catch (e) {
            reject(e);
        }
    }).then(() => {
        //job done, remove it and emit event
        return this.remove(job.id, 'run').then(() => {
            clearTimeout(timeOut);
            this.emit('queue ok', job);
        });
    }).catch(err => {
        //job failure, move job to appropriate queue
        return this.moveJob(job).then(j => {
            clearTimeout(timeOut);
            if (j.queue === this.failQueue) {
                this.emit('queue failure', err, j);
            } else {
                this.emit('queue error', err, j);
            }
        });
    });
};

/**
 * Move job from run queue to another queue
 * 
 * @param {any} job 
 * @returns 
 */
Queue.prototype.moveJob = function (job) {
    let self = this;

    return new Promise(function (resolve, reject) {
        let multi = self.client.multi();

        //check retry limit, decide next queue
        if (job.retry >= self.config.maxRetry) {
            multi.rpoplpush(self.runQueue, self.failQueue);
            job.queue = self.failQueue;
        } else {
            multi.rpoplpush(self.runQueue, self.workQueue);
            job.queue = self.workQueue;
        }

        //update job retry count
        job.retry++;
        multi.hset(`${self.prefix}:${job.id}`, 'retry', job.retry);
        multi.hset(`${self.prefix}:${job.id}`, 'queue', job.queue);

        multi.exec(function (err, res) {
            //see prototype.add on why verbose checks are needed
            //client error
            if (err) {
                //only return the first error
                reject(err[0]);
            } else if (res[0] === null) { //command error
                reject(new Error('job id missing from queue'));
            } else if (isNaN(parseInt(res[1], '10')) || res[1] > 0) {
                err = new Error('partial job data, retry count missing');
                reject(err);
            } else if (isNaN(parseInt(res[2], '10')) || res[2] > 0) {
                err = new Error('partial job data, queue name missing');
                reject(err);
            } else {
                resolve(job);
            }
        });
    });
};

/**
 * Recover job from runQueue back to start of workQueue
 * 
 * @returns 
 */
Queue.prototype.recoverJob = function () {
    let self = this;

    return this.count('run').then(count => {
        //nothing to do if last session shutdown gracefully
        if (count === 0) {
            return Promise.resolve();
        } else if (count > 1) { //by design there should only be at max 1 job in runQueue
            return Promise.reject(new Error('more than 1 job in queue, purge manually'));
        } else { //move the job to work queue, note that retry count does not increase
            return new Promise(function (resolve, reject) {
                self.client.rpoplpush(self.runQueue, self.workQueue, function (err, res) {
                    if (err) {
                        reject(err);
                    } else if (res === null) {
                        reject(new Error('job id missing from queue'));
                    } else {
                        resolve();
                    }
                });
            });

        }
    });
};

/**
 * Increment queue id and return it
 * 
 * @returns 
 */
Queue.prototype.nextId = function () {
    let self = this;

    return new Promise(function (resolve, reject) {
        self.client.incr(`${self.prefix}:id`, function (err, res) {
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
};

/**
 * Retrieve the next job on work queue, put it on the run queue
 * 
 * @returns 
 */
Queue.prototype.nextJob = function () {
    let self = this;

    return new Promise(function (resolve, reject) {
        self.bclient.brpoplpush(self.workQueue, self.runQueue, self.config.blockTimeout, function (err, id) {
            //client error
            if (err) {
                reject(err);
            } else if (id === null) { //blocking timeout, return special code
                resolve(self.status_timeout);
            } else {
                resolve(self.get(id));
            }
        });
    });
};

/**
 * Convert job data into a format supported by redis client
 * 
 * @param {any} job 
 * @returns 
 */
Queue.prototype.toClient = function (job) {
    // all values must be primitive type
    return {
        id: job.id,
        data: JSON.stringify(job.data),
        retry: job.retry,
        timeout: job.timeout,
        queue: job.queue
    };
};

/**
 * Convert redis data into the original job format
 * 
 * @param {any} job 
 * @returns 
 */
Queue.prototype.fromClient = function (job) {
    // values in redis are stored as string
    return {
        id: parseInt(job.id, 10),
        data: JSON.parse(job.data),
        retry: parseInt(job.retry, 10),
        timeout: parseInt(job.timeout, 10),
        queue: job.queue
    };
};

module.exports = Queue;